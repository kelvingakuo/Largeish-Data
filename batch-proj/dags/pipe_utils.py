from airflow.hooks.S3_hook import S3Hook
import logging
import os

def file_to_s3(filename, key, bucket_name):
	s3 = S3Hook()
	s3.load_file(filename = filename, key = key, bucket_name = bucket_name, replace = True)


def delete_file(location):
	if os.path.isfile(location):
		os.remove(location)
	else:
		logging.error(f"File not found in {location}")

