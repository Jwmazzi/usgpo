from . import bill_types

from arcgis.features import GeoAccessor
from arcgis.gis import GIS

from datetime import datetime, timedelta
import xml.etree.ElementTree as et
import pandas as pd
import requests
import json
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

    def get_gis(self):

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

        """ Return Most Recent Action for Input Bill Link """

        r = requests.get(status_link, params={'api_key': self.config["api_key"]})

        e = et.fromstring(r.content)
        l = next(e.iter('latestAction'))

        return next(l.iter('text')).text

    def process_package(self, package, keyword):

        api_key = self.config["api_key"]

        r = requests.get(package.get('packageLink'), params={'api_key': api_key}).json()

        member_list = r.get('members', [])

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
                'last_action': self.get_status(r['related']['billStatusLink'])
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

    def fetch_bills(self, past_days=7):

        self.get_gis()

        state_itm = self.gis.content.get(self.config["state_id"])
        state_lyr = state_itm.layers[0]
        state_df  = state_lyr.query(out_fields=['STATE_NAME', 'STATE_ABBR']).sdf

        bills_itm = self.gis.content.get(self.config["bills_id"])
        bills_lyr = bills_itm.tables[0]
        bills_lyr.delete_features(where='1=1')

        membs_itm = self.gis.content.get(self.config["membs_id"])
        membs_lyr = membs_itm.layers[0]
        membs_lyr.delete_features(where='1=1')

        term_dictionary = self.fetch_terms()

        bill_dfs = []
        bill_ids = []

        for category, keywords in term_dictionary.items():
            for bill_type, bill_desc in bill_types.items():

                past_time  = self.get_past_time(past_days)
                collection = self.get_collection('BILLS', past_time, bill_type)
                coll_df    = self.get_collection_df(collection, category, keywords)

                if len(coll_df) > 0:

                    coll_df['bill_type'] = bill_desc

                    for bill_number in coll_df['bill_number'].unique():
                        if bill_number not in bill_ids:
                            try:
                                bills = coll_df[coll_df['bill_number'] == bill_number]
                                sponsor = bills[bills['role'] == 'SPONSOR'].spatial.to_featureset().features[0]
                                bills_lyr.edit_features(adds=[sponsor])
                            except IndexError:
                                print(f'Expected Sponsor Missing: {bill_number}')

                    out_df = coll_df.merge(state_df, left_on='state', right_on='STATE_ABBR')
                    bill_dfs.append(out_df)

        pub_df = pd.concat(bill_dfs)
        for feature in pub_df.spatial.to_featureset():
            resp = membs_lyr.edit_features(adds=[feature])
            if not resp['addResults'][0]['success']:
                print(f'Dropped: {resp}')
