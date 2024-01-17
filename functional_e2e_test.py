import os
import configparser
import json

import boto3
from unittest import TestCase
import requests

s3 = boto3.client('s3')

print('Execute functional e2e test')


def extract_image_asset(assets) -> str:
    for asset in assets:
        if '.png' in asset:
            return os.path.basename(asset)


def extract_metadata_asset(assets) -> str:
    for asset in assets:
        if '.json' in asset:
            return os.path.basename(asset)


class FunctionalE2ETest(TestCase):

    config = None

    def setUp(self):
        # read bucket configs
        self.config = configparser.ConfigParser()
        self.config.read('resources/configuration.ini')
        # read expected test data configs
        self.expected_complete = [int(num) for num in self.config['Expected']['Complete'].split(',')]
        self.expected_ingesting = [int(num) for num in self.config['Expected']['Ingesting'].split(',')]
        self.expected_failed = [int(num) for num in self.config['Expected']['failed'].split(',')]
        # read sample catalog
        self.sample_catalog = self.read_assets_metadata()['data']
        # read output catalog
        url = self.config['API']['CatalogUrl']
        req = requests.get(url)
        self.catalog = json.loads(req.content)
        # read output products
        # get products
        url = self.config['API']['ProductUrl']
        req = requests.get(url)
        self.products = json.loads(req.content)

    def read_assets_metadata(self):
        bucket: str = self.config['Assets']['BucketName']
        key: str = self.config['Assets']['BucketKey']
        try:
            # read metadata
            response = s3.get_object(Bucket=bucket, Key=key)
            metadata = json.load(response['Body'])
            return metadata
        except Exception as e:
            print(e)
            print(
                'Error cleaning up object {} from bucket {}. Make sure they exist and in the same region.'
                .format(key, bucket))
            raise e

    @classmethod
    def tearDownClass(cls):
        config = configparser.ConfigParser()
        config.read('resources/configuration.ini')
        # read meta data
        bucket: str = config['Assets']['BucketName']
        key: str = config['Assets']['BucketKey']
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            metadata = json.load(response['Body'])
            # clean up files in inputs
            bucket: str = config['Inputs']['BucketName']
            key: str = config['Inputs']['BucketKey']
            prefix: str = config['Inputs']['Prefix']
            for catalog in metadata['data']:
                catalog_id: int = catalog['id']
                key: str = f'{prefix}/sample-catalog-{catalog_id}.json'
                s3.delete_object(Bucket=bucket, Key=key)
        except Exception as e:
            print(e)
            print(
                'Error cleaning up object {} from bucket {}. Make sure they exist and in the same region.'
                .format(key, bucket))
            raise e

    def test_catalog_size(self):
        self.assertEqual(len(self.sample_catalog), len(self.catalog))

    def test_catalog_status(self):
        for entry in self.catalog:
            if entry['status'] == 'ingesting':
                self.assertIn(entry['id'], self.expected_ingesting)
            elif entry['status'] == 'complete':
                self.assertIn(entry['id'], self.expected_complete)
            elif entry['status'] == 'failed':
                self.assertIn(entry['id'], self.expected_failed)

    def test_products_size(self):
        self.assertEqual(len(self.products), len(self.expected_complete))

    def test_product_status(self):
        products_map = {item['input_id']: item for item in self.products}
        for entry in self.catalog:
            if entry['status'] == 'complete':
                input_id: str = entry['id']
                self.assertIn(input_id, products_map.keys())
                self.assertIn(products_map[input_id]['status'], ['processing', 'complete'])
            else:
                self.assertNotIn(entry['id'], products_map.keys())

    def test_product_image_assets(self):
        catalog_map = {item['id']: item for item in self.catalog}
        for product in self.products:
            if product['status'] == 'complete':
                product_image = extract_image_asset(product['assets'])
                self.assertTrue(product_image)
                catalog_image = extract_image_asset(catalog_map.get(product['input_id'])['assets'])
                self.assertEqual(product_image.removesuffix('-product.png'),
                                 catalog_image.removesuffix('-catalog.png'))

    def test_product_metadata_assets(self):
        catalog_map = {item['id']: item for item in self.catalog}
        for product in self.products:
            if product['status'] == 'complete':
                product_image = extract_metadata_asset(product['assets'])
                self.assertTrue(product_image)
                catalog_image = extract_metadata_asset(catalog_map.get(product['input_id'])['assets'])
                self.assertEqual(product_image.removesuffix('-product-metadata.json'),
                                 catalog_image.removesuffix('-catalog-metadata.json'))
