from . import bill_types

from arcgis.features import GeoAccessor
from arcgis.gis import GIS

from datetime import datetime, timedelta
import xml.etree.ElementTree as et
from itertools import chain
import pandas as pd
import traceback
import requests
import json
import time
import sys

import warnings
warnings.filterwarnings("ignore")


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

    @staticmethod
    def process_feature_edits(feature_list, feature_layer, operation):

        pushed = 0

        for feature in feature_list:

            try:

                edit_feature = {
                    'geometry': feature['SHAPE'],
                    'attributes': feature
                }

                if operation == 'update':
                    response = feature_layer.edit_features(updates=[edit_feature])['updateResults']
                else:
                    response = feature_layer.edit_features(adds=[edit_feature])['addResults']

                if not response[0]['success']:
                    print(response)
                else:
                    pushed += 1

            except:
                print(traceback.format_exc())

        print(f'Processed {pushed} of {len(feature_list)} {operation.upper()}')

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
            'bio_link': f'https://bioguideretro.congress.gov/Home/MemberDetails?memIndex={sponsor_element.find("bioguideId").text}',
            'bio_id': sponsor_element.find("bioguideId").text,
            'party': sponsor_element.find('party').text,
            'state': sponsor_element.find('state').text
        }

    def parse_sponsor_xml(self, sponsor_element, introduced_date):

        # TODO - Add Parameter to Specify Sponsor or Cosponsor (Maybe Just Have Original Cosponsor Flag)

        return {
            'full_name': sponsor_element.find('fullName').text,
            'original_cosponsor': 'True',
            'sponsor_date': introduced_date,
            'bio_link': f'https://bioguideretro.congress.gov/Home/MemberDetails?memIndex={sponsor_element.find("bioguideId").text}',
            'bio_id': sponsor_element.find("bioguideId").text,
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
            try:
                r = requests.get(package.get('packageLink'), params={'api_key': self.config['api_key']}).json()

                sponsor_list = self.process_bill_status(r['related']['billStatusLink'])
                sponsor_list = self.process_package(package, sponsor_list)

                data.append(sponsor_list)

            # TODO - Why Do Some Entries Lack the Related Bill Status Link Attribute?
            except KeyError as key_err:
                print(f'Skipping Package: {package} Based on Key Error: {key_err}')

        if not data:
            return None

        df = pd.DataFrame(list(chain(*data)))

        df['sponsor_date'] = pd.to_datetime(df['sponsor_date'])
        df['date_issued']  = pd.to_datetime(df['date_issued'])
        df['last_date']    = pd.to_datetime(df['last_date'])

        df['unique_id'] = df['bio_id'] + df['package_id']

        return df

    def fetch_bills(self, past_days):

        bill_dfs = []

        past_time = self.get_past_time(past_days)
        past_ts   = pd.to_datetime(past_time).replace(hour=0, minute=0, second=0, tzinfo=None)

        for bill_type, bill_desc in bill_types.items():

            collection = self.get_collection('BILLS', past_time, bill_type)
            collect_df = self.get_collection_df(collection)

            if isinstance(collect_df, pd.DataFrame):
                collect_df = collect_df[collect_df.last_date >= past_ts]
                if len(collect_df) > 0:
                    print(f'Found {len(collect_df)} {bill_desc} Entries')
                    collect_df['bill_type'] = bill_desc
                    bill_dfs.append(collect_df)

        return bill_dfs

    def handle_updates(self, edit_lyr, old_sdf, new_sdf, id_field):

        if not len(old_sdf):
            self.process_feature_edits(new_sdf.to_dict('records'), edit_lyr, 'add')

        else:
            merged = old_sdf.merge(new_sdf, on=id_field, how='outer', indicator=True)
            add_df = merged[merged['_merge'] == 'right_only']
            upd_df = merged[merged['_merge'] == 'both']

            adds = add_df[[c for c in add_df.columns if not c.endswith('_x')]]
            adds.columns = adds.columns.str.replace('_y', '')
            adds.drop(columns=['_merge'], inplace=True)
            upds = upd_df[[c for c in upd_df.columns if not c.endswith('_x')]]
            upds.columns = upds.columns.str.replace('_y', '')
            upds.drop(columns=['_merge'], inplace=True)

            adds.fillna(0, inplace=True)
            upds.fillna(0, inplace=True)

            if len(upds):
                self.process_feature_edits(upds.to_dict('records'), edit_lyr, 'update')

            if len(adds):
                self.process_feature_edits(adds.to_dict('records'), edit_lyr, 'add')

    @staticmethod
    def delete(lyr, df, create_field, oid_field, max_date):

        del_oids = df[df[create_field] < max_date][oid_field].to_list()

        if del_oids:
            del_list = ','.join([str(i) for i in del_oids])
            res = lyr.delete_features(del_list)['deleteResults']
            print(f"Deleted {len([i for i in res if i['success']])} rows")
        else:
            print('No Records Found for Deletion')

    def run_solution(self, past_days=1, max_age=14):

        start = time.time()

        try:
            # Connect to ArcGIS Online
            self.set_gis()

            # Collect Most Recent Bill Data
            bills_df_list = self.fetch_bills(past_days)

            if len(bills_df_list) == 0:
                print(f'Nothing Found in USGPO For Past {past_days} Day(s).')
                return
            else:
                bills_df = pd.concat(bills_df_list)

            # Fetch Existing Sponsor Data as a Data Frame
            sponsor_itm    = self.gis.content.get(self.config["sponsors"])
            sponsor_lyr    = sponsor_itm.layers[0]
            old_sponsor_df = sponsor_lyr.query().sdf

            # Fetch State Boundaries as a Data Frame
            state_itm = self.gis.content.get(self.config["state_id"])
            state_lyr = state_itm.layers[0]
            state_df  = state_lyr.query(out_fields=['NAME', 'STATE_ABBR']).sdf

            # Prepare Newly Fetched Bill Data Frame for Insertion
            new_sponsor_df = bills_df.merge(state_df, left_on='state', right_on='STATE_ABBR')

            # Push Edits to ArcGIS Online
            self.handle_updates(sponsor_lyr, old_sponsor_df, new_sponsor_df, 'unique_id')

            # Remove Anything Older Than Max Age
            past_date = (datetime.utcnow() - timedelta(days=max_age))
            if len(old_sponsor_df):
                self.delete(sponsor_lyr, old_sponsor_df, 'last_date', sponsor_lyr.properties.objectIdField, past_date)

        except:
            print(traceback.format_exc())

        finally:
            print(f'Process Ran in {round((time.time() - start) / 60, 2)} Minutes')

