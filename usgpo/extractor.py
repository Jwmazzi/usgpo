from . import bill_types

from arcgis.features import GeoAccessor
from arcgis.gis import GIS

from datetime import datetime, timedelta
import xml.etree.ElementTree as et
from itertools import chain
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

    @staticmethod
    def batches(l, n):

        for i in range(0, len(l), n):
            yield l[i:i + n]

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

    def parse_cosponsor_xml(self, sponsor_element):

        # TODO - Add Parameter to Specify Sponsor or Cosponsor (Maybe Just Have Original Cosponsor Flag)

        return {
            'full_name': sponsor_element.find('fullName').text,
            'original_cosponsor': sponsor_element.find('isOriginalCosponsor').text,
            'sponsor_date': sponsor_element.find('sponsorshipDate').text,
            'bio_id': f'https://bioguideretro.congress.gov/Home/MemberDetails?memIndex={sponsor_element.find("bioguideId").text}',
            'party': sponsor_element.find('party').text,
            'state': sponsor_element.find('state').text
        }

    def parse_sponsor_xml(self, sponsor_element, introduced_date):

        # TODO - Add Parameter to Specify Sponsor or Cosponsor (Maybe Just Have Original Cosponsor Flag)

        return {
            'full_name': sponsor_element.find('fullName').text,
            'original_cosponsor': 'True',
            'sponsor_date': introduced_date,
            'bio_id': f'https://bioguideretro.congress.gov/Home/MemberDetails?memIndex={sponsor_element.find("bioguideId").text}',
            'party': sponsor_element.find('party').text,
            'state': sponsor_element.find('state').text
        }

    def process_bill_status(self, status_link):

        # TODO - Replace Next with Find Throughout This Function
        #      - et.fronstring(respon.content).find('bill) Need to be How We Define "Root"

        # Fetch Bill Status XML & Parse
        resp = requests.get(status_link, params={'api_key': self.config["api_key"]})
        root = et.fromstring(resp.content)

        introduced  = next(root.iter('introducedDate')).text

        subjects    = next(next(next(root.iter('subjects')).iter('billSubjects')).iter('legislativeSubjects'))
        subject_str = ', '.join([i[0].text for i in subjects.iter('item')])

        policy      = next(root.iter('policyArea'))
        policy_area = policy[0].text if len(policy) == 1 else None

        latest             = root.find('bill').find('latestAction')
        latest_action      = next(latest.iter('text')).text
        latest_action_date = next(latest.iter('actionDate')).text

        sponsors = []
        for sponsor in next(root.iter('cosponsors')).iter('item'):
            sponsor = self.parse_cosponsor_xml(sponsor)
            sponsor.update({
                'last_date':   latest_action_date,
                'last_action': latest_action,
                'policy_area': policy_area,
                'subjects':    subject_str[:999],
            })
            sponsors.append(sponsor)

        first_sponsor = self.parse_sponsor_xml(next(root.iter('sponsors')).find('item'), introduced)
        first_sponsor.update({
            'last_date': latest_action_date,
            'last_action': latest_action,
            'policy_area': policy_area,
            'subjects': subject_str[:999],
        })
        sponsors.append(first_sponsor)

        return sponsors

    def process_package(self, package, sponsor_list):

        api_key = self.config["api_key"]
        resp    = requests.get(package.get('packageLink'), params={'api_key': api_key}).json()

        for sponsor in sponsor_list:
            sponsor.update({
                'committees': ', '.join([c['committeeName'] for c in resp.get('committees', [])])[:999],
                'other_title': ', '.join([c['title'] for c in resp.get('shortTitle', [])])[:999],
                'link': f"{resp['download']['pdfLink']}?api_key={api_key}",
                'date_issued': package['dateIssued'],
                'bill_number': resp['billNumber'],
                'package_id': resp['packageId'],
                'title': resp['title'][:999]
            })

        return sponsor_list

    def get_collection_df(self, collection):

        data = []

        for package in collection:

            r = requests.get(package.get('packageLink'), params={'api_key': self.config['api_key']}).json()

            sponsor_list = self.process_bill_status(r['related']['billStatusLink'])
            sponsor_list = self.process_package(package, sponsor_list)

            data.append(sponsor_list)

        if not data:
            return pd.DataFrame()
        else:
            data = list(chain(*data))

        df = pd.DataFrame(data)

        df['sponsor_date'] = pd.to_datetime(df['sponsor_date'])
        df['date_issued'] = pd.to_datetime(df['date_issued'])
        df['last_date'] = pd.to_datetime(df['last_date'])

        return df

    def fetch_bills(self, past_days):

        bill_dfs = []
        bill_fts = []
        bill_ids = []

        for bill_type, bill_desc in bill_types.items():

            past_time  = self.get_past_time(past_days)
            collection = self.get_collection('BILLS', past_time, bill_type)
            collect_df = self.get_collection_df(collection)

            if len(collect_df) > 0:

                collect_df['bill_type'] = bill_desc

                for bill_number in collect_df['bill_number'].unique():
                    if bill_number not in bill_ids:
                        bills   = collect_df[collect_df['bill_number'] == bill_number]
                        bill_fts.append(bills.spatial.to_featureset().features[0])

                bill_dfs.append(collect_df)

        return bill_dfs, bill_fts

    def process_edits(self, bills_df, bills_fs):

        bills_itm = self.gis.content.get(self.config["bills_id"])
        bills_lyr = bills_itm.tables[0]
        bills_lyr.delete_features(where='1=1')

        print('Pushing Bill Edits')
        for bill in bills_fs:
            response = bills_lyr.edit_features(adds=[bill])['addResults'][0]
            if not response['success']:
                print(f'Edit Failed: {response}')

        membs_itm = self.gis.content.get(self.config["membs_id"])
        membs_lyr = membs_itm.layers[0]
        membs_lyr.delete_features(where='1=1')

        state_itm = self.gis.content.get(self.config["state_id"])
        state_lyr = state_itm.layers[0]
        state_df  = state_lyr.query(out_fields=['STATE_NAME', 'STATE_ABBR']).sdf

        concat_df = pd.concat(bills_df)
        member_df = concat_df.merge(state_df, left_on='state', right_on='STATE_ABBR')

        print('Pushing Member Edits')
        member_sets = self.batches(member_df.spatial.to_featureset().features, 500)
        for idx, member_set in enumerate(member_sets):
            try:
                response = membs_lyr.edit_features(adds=member_set)['addResults']
                print(f'{len([e for e in response if e["success"]])} Edits Passed')
            except:
                print(f'Edit Batch {idx} Failed')

    def run_solution(self, past_days=30):

        start = time.time()

        self.set_gis()

        bill_df, bill_fs = self.fetch_bills(past_days)

        for bill in bill_df:
            print(len(bill))
            print(bill_df[0].iloc[0])

        # self.process_edits(bill_df, bill_fs)

        print(f'Process Ran in {round((time.time() - start) / 60, 2)} Minutes')

