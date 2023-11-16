"""Filter Scan with Lambda

"""
import os
import io
import time
import json

from boto3 import Session
from botocore.config import Config
from boto3.s3.transfer import TransferConfig, MB
import pandas as pd


def lambda_handler(event, context):
    response = main(event)
    return response

def main(event):
    # parse arguments
    s3key = event['s3key']  # 'access_method_benchmark/shards-1GB/lineitem.1.csv'
    select_fields = event['select_fields'].split('|')  # "_0|_5" -> ['_0', '_5']
    filter_expr = event['filter_expr']  # "_0 == '1'"
    # s3key = event['s3key'] = 'access_method_benchmark/shards-1GB/lineitem.1.csv'
    # select_fields = ['_0', '_5']
    # filter_expr = "_0 == '1'"
    file_format = "CSV"
    # launch query
    df, metrics = run(s3key=s3key, select_fields=select_fields, filter_expr=filter_expr, file_format=file_format)
    # encode results
    df_str = df.to_csv(sep='|', lineterminator='#', index=False)
    met_str = str(metrics)
    return str(len(met_str)) + met_str + df_str

def run(s3key, select_fields, filter_expr, file_format):
    """Fetch file stream from S3 and filter according to expression, return a dataframe
    :param s3key: The object key to select against
    :param select_fields: The fileds to keep (Projection)
    :param filter_expr: The filtering expression (Selection)
    """
    # init metrics 
    metrics = {"time_to_first_record_response": 0,
        "time_to_first_record_response": 0, 
        "bytes_returned": 0}
    start_time = time.time()

    # init dataframe as global buffer
    global_df = pd.DataFrame()

    # init S3 client
    cfg = Config(region_name="us-east-2", parameter_validation=False, max_pool_connections=10)
    session = Session()
    s3 = session.client('s3', config=cfg)
    S3_BUCKET_NAME = "s3filter-289785222077"

    # fetch file chunck
    table_data = io.BytesIO()
    config = TransferConfig(
        multipart_chunksize=8 * MB,
        multipart_threshold=8 * MB
    )
    s3.download_fileobj(
        Bucket=S3_BUCKET_NAME,
        Key=s3key,
        Fileobj=table_data,
        Config=config
    )

    metrics["num_http_get_requests"] = 1
    # parse and filter stream
    chunksize = 10000  # number of rows 

    if file_format == 'CSV':
        metrics["time_to_first_record_response"] = time.time() - start_time
        table_data.seek(0)
        for df in pd.read_csv(table_data, delimiter='|', lineterminator='\n',
                                header=None,
                                dtype=str,
                                engine='c', quotechar='"', na_filter=False, compression=None, low_memory=False,
                                skiprows=1,
                                chunksize=chunksize):
            # Get read bytes
            metrics["bytes_returned"] += table_data.tell()
            # filter
            df.columns = ["_" + str(col) for col in df.columns]  # add prefix
            filtering_result = df.eval(filter_expr)
            df = df[filtering_result]
            if df.shape[0] > 0:
                global_df = pd.concat([global_df, df[select_fields]], ignore_index=True)               
            
        metrics["time_to_last_record_response"] = time.time() - start_time
    else:
        raise Exception("Unrecognized input type '{}'".format(file_format))

    return global_df, metrics
