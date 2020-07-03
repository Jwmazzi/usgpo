from . import bill_types

from arcgis.features import GeoAccessor
from arcgis.gis import GIS

from datetime import datetime, timedelta
import xml.etree.ElementTree as et
import pandas as pd
import requests
import json
import time
import sys


class Extractor(object):

    def __init__(self, config):

        self.config = self.read_config(config)
        self.gis = None

    @staticmethod
    def read_config(config):

        try:
            return config if isinstance(config, dict) else json.load(open(config))

        except ValueError as val_err:
            print(f'Configuration Input "{config}" is Not Valid: {val_err}')
            sys.exit(1)

    @staticmethod
    def get_past_time(days):

        old_time = datetime.utcnow() - timedelta(days=days)
        iso_time = f"{old_time.isoformat().split('.')[0]}Z"

        return iso_time

    def set_gis(self):

        self.gis = GIS(
            self.config["esri_url"],
            self.config["username"],
            self.config["password"]
        )

    def get_collection(self, col_type, last_mod, doc_class):

        payload = {
            'congress': self.config["congress"],
            'api_key': self.config["api_key"],
            'docClass': doc_class,
            'pageSize': 100,
            'offset': 0
        }

        packages = []

        response = requests.get(f'{self.config["api_url"]}/{col_type}/{last_mod}', params=payload).json()

        if response['packages']:
            packages += response['packages']

        # Paginate Through All Available Packages
        if response['nextPage']:
            while True:
                response = requests.get(response['nextPage'], params={'api_key': self.config["api_key"]}).json()
                packages += response['packages']
                if not response['nextPage']:
                    break

        return packages

    def get_status(self, status_link):

        r = requests.get(status_link, params={'api_key': self.config["api_key"]})

        e = et.fromstring(r.content)

        latest = next(e.iter('latestAction'))
        action = next(latest.iter('text')).text
        date   = next(latest.iter('actionDate')).text

        return action, date

    def process_package(self, package, keyword):

        api_key = self.config["api_key"]

        r = requests.get(package.get('packageLink'), params={'api_key': api_key}).json()

        member_list = r.get('members', [])

        latest_action, latest_date = self.get_status(r['related']['billStatusLink'])

        for member in member_list:
            member.update({
                'link': f"{r['download']['pdfLink']}?api_key={api_key}",
                'title': r['title'],
                'package_id': r['packageId'],
                'bill_number': r['billNumber'],
                'keyword': keyword,
                'dateIssued': package['dateIssued'],
                'committees': ', '.join([c['committeeName'] for c in r.get('committees', [])]),
                'other_title': ', '.join([c['title'] for c in r.get('shortTitle', [])]),
                'last_action': latest_action,
                'last_date': latest_date
            })

        return member_list

    def get_collection_df(self, collection, category, keywords):

        data = []

        for package in collection:
            try:
                for keyword in [k.lower() for k in keywords]:
                    if keyword in package['title'].lower():
                        members = self.process_package(package, keyword)
                        for member in members:
                            data.append(member)
            except KeyError as key_err:
                print(f'Dropped Record Expecting Key: {key_err}')

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)

        df['last_date'] = pd.to_datetime(df['last_date'])
        df['title'] = df['title'].apply(lambda x: f'{x[:250]}')
        df['category'] = category

        return df

    def fetch_terms(self):

        category_itm = self.gis.content.get(self.config['categ_id'])
        category_fs  = category_itm.tables[0].query().features
        keyword_fs   = category_itm.tables[1].query().features

        categories = {k: [] for k in [c.attributes['category'] for c in category_fs]}

        for feature in keyword_fs:
            categories[feature.attributes['category']].append(feature.attributes['keyword'])

        return categories

    def fetch_bills(self, term_dictionary, past_days=60):

        print(f'Fetching Bill Data for {len(term_dictionary.keys())} Categories')

        bill_dfs = []
        bill_fts = []
        bill_ids = []

        for category, keywords in term_dictionary.items():
            for bill_type, bill_desc in bill_types.items():

                past_time  = self.get_past_time(past_days)
                collection = self.get_collection('BILLS', past_time, bill_type)
                collect_df = self.get_collection_df(collection, category, keywords)

                if len(collect_df) > 0:

                    collect_df['bill_type'] = bill_desc

                    for bill_number in collect_df['bill_number'].unique():
                        if bill_number not in bill_ids:
                            bills   = collect_df[collect_df['bill_number'] == bill_number]
                            sponsor = bills[bills['role'] == 'SPONSOR'].spatial.to_featureset().features
                            if len(sponsor) == 1:
                                bill_fts.append(sponsor[0])

                    bill_dfs.append(collect_df)

        return bill_dfs, bill_fts

    def process_edits(self, bills_df, bills_fs):

        print('Pushing Edits')

        bills_itm = self.gis.content.get(self.config["bills_id"])
        bills_lyr = bills_itm.tables[0]
        bills_lyr.delete_features(where='1=1')

        response = bills_lyr.edit_features(adds=bills_fs)
        print(f'Processed {len([e for e in response["addResults"] if e])} Bill Edits')

        membs_itm = self.gis.content.get(self.config["membs_id"])
        membs_lyr = membs_itm.layers[0]
        membs_lyr.delete_features(where='1=1')

        state_itm = self.gis.content.get(self.config["state_id"])
        state_lyr = state_itm.layers[0]
        state_df  = state_lyr.query(out_fields=['STATE_NAME', 'STATE_ABBR']).sdf

        concat_df = pd.concat(bills_df)
        member_df = concat_df.merge(state_df, left_on='state', right_on='STATE_ABBR')
        respponse = membs_lyr.edit_features(adds=member_df.spatial.to_featureset())
        print(f'Processed {len([e for e in respponse["addResults"] if e])} Member Edits')

    def run_solution(self):

        start = time.time()

        self.set_gis()

        term_dictionary  = self.fetch_terms()
        bill_df, bill_fs = self.fetch_bills(term_dictionary)

        self.process_edits(bill_df, bill_fs)

        print(f'Process Ran in {round((time.time() - start) / 60, 2)} Minutes')
