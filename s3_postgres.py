"""
This script is a sample for fetching objects from an S3 Bucket.
Performing actions on the object e.g. incase of PDF get no. of pages
Persisting results to a DB
"""
import os
import glob
import sys
import time
import boto3
import psycopg2
from psycopg2.extras import execute_values
from collections import OrderedDict
import logging
import typing as T

# Logger Settings
FORMAT = "%(asctime)-15s %(message)s"
logging.basicConfig(format=FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

conf = {
    "s3_bucket": "",
    "s3_path": "",
    "db_port": 5432,
    "db_name": "",
    "db_host": "",
    "db_user": "",
    "db_password": "",
    "table_name": "",
    "download_dir": "",
}

flags = {}

s3_session = boto3.Session(profile_name="")  # Check ~/.aws/credentials ,  ~/.aws/config
s3_client = s3_session.client("s3")
s3_bucket = s3_session.resource("s3")


def iterate_bucket_items(bucket):
    """
    Generator that iterates over all objects in a given s3 bucket

    See http://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.list_objects_v2
    for return data format
    :param bucket: name of s3 bucket
    :return: dict of metadata for an object
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=conf["s3_path"])
    for page in page_iterator:
        if page["KeyCount"] > 0:
            for item in page["Contents"]:
                yield item


def get_sql_connection():
    return psycopg2.connect(
        user=conf["db_user"],
        password=conf["db_passsord"],
        host=conf["db_host"],
        port=conf["db_port"],
        database=conf["db_name"],
    )


def persist_to_db(records: T.List[T.OrderedDict]):
    conn = get_sql_connection()
    cursor = conn.cursor()
    columns = records[0].keys()
    query = "INSERT INTO {} ({}) VALUES %s".format(
        conf["table_name"], ",".join(columns)
    )
    values = [[value for value in record.values()] for record in records]
    execute_values(cursor, query, values)
    conn.commit()
    cursor.close()


def get_pdf_page_count(record):
    """
    Get file location from record. Analyse PDF and add value to object.
    """

def get_file_names_from_db(limit=10000):
    """
    Modify for ORDER BY, SKIP etc.
    """
    cursor = get_sql_connection().cursor()
    query = "SELECT DISTINCT file_name from {} LIMIT {}".format(
        conf["table_name"], limit
    )
    cursor.execute(query)
    results = cursor.fetchall()
    return set(results)


def create_record() -> T.OrderedDict:
    """
    Should have same fields as destination db table columns
    """
    return OrderedDict({"s3_path": None, "s3_bucket": None, "file_name": None})


def main():
    logger.info("Started Execution")
    for i in iterate_bucket_items(bucket=conf["s3_bucket"]):
        record = create_record()
        if i["Size"] == 0:
            continue
        record["file_name"] = str.replace(
            i["Key"], conf["s3_path"], conf["download_dir"]
        )
        s3_bucket.download_file(i["Key"], record["file_name"])
        logger.info("Downloaded {}".format(record["file_name"]))
        # Perform operations on record. 
        # Pipeline should be like a DAG 
        # get_pdf_page_count(record)
        # Persist per loop or as a batch.
        # persist_to_db([record])

    logger.info("Finished Execution")
    exit()


if __name__ == "__main__":
    main()
