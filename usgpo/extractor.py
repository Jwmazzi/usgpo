from . import bill_types

from datetime import datetime, timedelta
from arcgis.gis import GIS
from arcgis.features import GeoAccessor
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
            'docClass': doc_class,
            'pageSize': 100,
            'congress': 116,
            'api_key': self.config["api_key"],
            'offset': 0
        }

        response = requests.get(f'{self.config["api_url"]}/{col_type}/{last_mod}', params=payload)

        return response.json()

    def process_package(self, package_link):

        api_key = self.config["api_key"]

        payload = {
            'api_key': api_key
        }

        response = requests.get(package_link, params=payload)
        data = response.json()

        member_list = data['members']
        for member in member_list:
            member.update({
                'link': f"{data['download']['pdfLink']}?api_key={api_key}",
                'title': data['title'],
                'package_id': data['packageId'],
                'bill_number': data['billNumber']
            })

        return member_list

    def get_collection_df(self, collection, keywords):

        data = []
        for p in collection.get('packages'):
            if any([k in p.get('title').lower() for k in keywords]):
                members = self.process_package(p.get('packageLink'))
                for member in members:
                    data.append(member)

        df = pd.DataFrame(data)

        return df

    def update_unique_packages(self, df, lyr):

        for id in df['package_id'].unique():
            update_df = df[df['package_id'] == id]
            first_feature = update_df.spatial.to_featureset().features[0]
            lyr.edit_features(adds=[first_feature])


    def fetch_bills(self, keywords, past_days=7):

        self.get_gis()

        state_itm = self.gis.content.get(self.config["state_id"])
        state_lyr = state_itm.layers[0]
        state_df  = state_lyr.query(out_fields=['STATE_NAME', 'STATE_ABBR']).sdf

        bills_itm = self.gis.content.get(self.config["bills_id"])
        bills_lyr = bills_itm.tables[0]
        bills_lyr.delete_features(where='1=1')

        bill_dfs = []

        for bill_type in bill_types:

            past = self.get_past_time(past_days)
            coll = self.get_collection('BILLS', past, bill_type)
            df   = self.get_collection_df(coll, keywords)

            if len(df) > 0:
                print(f'Found Results for {bill_type.upper()}')

                self.update_unique_packages(df, bills_lyr)

                out_df = df.merge(state_df, left_on='state', right_on='STATE_ABBR')
                bill_dfs.append(out_df)

        # print('Publishing Bills Feature Layer')
        # pub_df = pd.concat(bill_dfs)
        # pub_df.spatial.to_featurelayer(f'Keyword_Bills', gis=self.gis)
