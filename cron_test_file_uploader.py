import configparser
import json
import time

import boto3
from unittest import TestCase

print('Executing cron test file uploader')

s3 = boto3.client('s3')


class CronTestFileUploader(TestCase):

    def setUp(self):
        self.config = configparser.ConfigParser()
        self.config.read('resources/configuration.ini')

    def tearDown(self):
        pass

    def test_upload_sample_files(self):
        try:
            # read input assets metadata
            metadata = self.read_assets_metadata()
            self.assertTrue(metadata['data'])
            for catalog in metadata['data']:
                self.assertEqual(catalog['type'], "sample-catalog")
                # write output to S3
                self.upload_catalog(catalog)


        except Exception as e:
            print(e)
            raise e

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
                'Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region '
                'as this function.'.format(
                    key, bucket))
            raise e

    def upload_catalog(self, catalog):
        catalog_id: int = catalog['id']
        bucket: str = self.config['Inputs']['BucketName']
        key: str = self.config['Inputs']['BucketKey']
        prefix: str = self.config['Inputs']['Prefix']
        try:
            upload_object_key = f'{prefix}/sample-catalog-{catalog_id}.json'
            catalog['timestamp'] = time.time()
            response = s3.put_object(Bucket=bucket,
                                     Key=upload_object_key,
                                     Body=json.dumps(catalog))
            self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
            response = s3.get_object(Bucket=bucket, Key=upload_object_key)
            catalog = json.load(response['Body'])
            self.assertEqual(catalog['type'], 'sample-catalog')
            self.assertEqual(catalog['id'], catalog_id)
        except Exception as e:
            print(e)
            print(
                'Error putting object {} into bucket {}. Make sure they exist and your bucket is in the same region '
                'as this function.'.format(
                    key, bucket))
            raise e
