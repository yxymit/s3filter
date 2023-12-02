# -*- coding: utf-8 -*-
"""Indexing Benchmark 

"""

import os
import sys

from s3filter import ROOT_DIR
from s3filter.op.collate import Collate
from s3filter.op.sql_table_scan import SQLTableScanLambda
from s3filter.plan.query_plan import QueryPlan
from s3filter.sql.format import Format
from s3filter.util.test_util import gen_test_id
import boto3
from datetime import datetime, timedelta
import time
from s3filter.util.constants import *


# define lambda function name and region
Lambda_Function_Name = 'demo_layers'
Lambda_Region_Name = 'us-east-2'


# lambda cost
# for 512GB memory, arm: <https://aws.amazon.com/lambda/pricing/>
COST_LAMBDA_DURATION_PER_SECOND = 0.0000067
COST_LAMBDA_REQUEST_PER_REQ = 0.0000002
# EC2 in and out different AZ
COST_LAMBDA_DATA_TRANSFER_PER_GB = 0.01

def main(filter_expr):
    path = 'access_method_benchmark/shards-1GB'
    select_fields = "_0|_5"  # [l_orderkey, l_extendedprice]
    start_part = 1
    table_parts = 10
    chunk_size = 10000
    run(parallel=True, 
        start_part=start_part, table_parts=table_parts, path=path, 
        select_fields=select_fields, filter_expr=filter_expr, chunk_size=chunk_size)


def run(parallel, start_part, table_parts, path, select_fields, filter_expr, chunk_size):
    secure = False
    use_native = False
    use_pandas = True
    buffer_size = 0
    print('')
    print("Lambda Scan Benchmark")
    print("------------------")

    # Query plan
    query_plan = QueryPlan(is_async=parallel, buffer_size=buffer_size)

    scan = []
    # s3key, select_fields, filter_expr, name, query_plan, log_enabled
    for p in range(start_part, start_part + table_parts):
        scan.append(query_plan.add_operator(
            SQLTableScanLambda(s3key='{}/lineitem.{}.csv'.format(path, p),
                        select_fields=select_fields,
                        filter_expr=filter_expr,
                        name='lambda_scan_{}'.format(p),
                        query_plan=query_plan,
                        log_enabled=False, chunk_size=chunk_size))
                    )

    collate = query_plan.add_operator(
        Collate('collate', query_plan, False))


    for p, opt in enumerate(scan):
        opt.connect(collate)

    # Plan settings
    print('')
    print("Settings")
    print("--------")
    print('')
    print('use_pandas: {}'.format(use_pandas))
    print("table parts: {}".format(table_parts))
    print('')

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../benchmark-output"), gen_test_id() + "-" + str(table_parts))

    # Start the query
    # START_TIME is for cloudwatch metrics query, we add extra 2s buffer
    lambda_start_time = datetime.utcnow()

    query_plan.execute()
    print('Done')

    lambda_end_time = datetime.utcnow()


    tuples = collate.tuples()
    # collate.print_tuples(tuples)

    # Write the metrics
    query_plan.print_metrics()

    # print lambda cost
    lambda_cost = get_lambda_cost(lambda_start_time, lambda_end_time)

    # print total cost
    print("Total Cost")
    print("--------")
    cost, bytes_scanned, bytes_returned, http_requests, rows = query_plan.cost()
    print("${0:.8f}".format(lambda_cost + cost))
    print('')

    # Shut everything down
    query_plan.stop()

def get_lambda_cost(start_time, end_time):
    '''
    get metrics from cloud watch, and calculate the actual cost
    '''
    retries = 0 
    max_retries = 10
    response = None
    cloudwatch = boto3.client('cloudwatch', region_name= Lambda_Region_Name)
    print("wait 60 seconds to make sure cloudWatch metrics have updated")
    time.sleep(60)
    while True:
        # Define end time
        response = cloudwatch.get_metric_statistics(
            Namespace='AWS/Lambda',
            MetricName='Duration',
            Dimensions=[{'Name': 'FunctionName', 'Value': Lambda_Function_Name}],
            StartTime=start_time - timedelta(seconds=60),
            EndTime=end_time + timedelta(seconds=60),
            Period=120,   # Period for statistics (60 seconds - 1 minutes)
            Statistics=['Sum']  # Retrieve the maximum value
        )
        if len(response['Datapoints']) == 0:
            retries += 1
            if retries > max_retries:
                raise RuntimeError("Exceeded max retries")

            print("wait 30 seconds to make sure cloudWatch metrics have updated")
            time.sleep(30)
        else:
            break

    sum_duration = sum(datapoint['Sum'] for datapoint in response['Datapoints'])
    lambda_duration_cost = sum_duration / 1000 * COST_LAMBDA_DURATION_PER_SECOND

    # get invocations metrics
    invocations_response = cloudwatch.get_metric_statistics(
        Namespace='AWS/Lambda',
        MetricName='Invocations',
        Dimensions=[{'Name': 'FunctionName', 'Value': Lambda_Function_Name}],
        StartTime=start_time - timedelta(seconds=60),
        EndTime=end_time + timedelta(seconds=60),
        Period=120,
        Statistics=['Sum']
    )

    invocations_sum = sum(datapoint['Sum'] for datapoint in invocations_response['Datapoints'])
    lambda_invocation_cost = invocations_sum * COST_LAMBDA_REQUEST_PER_REQ

    # print lambda_total_cost
    lambda_total_cost = lambda_duration_cost + lambda_invocation_cost

    print("Lambda Cost")
    print("--------")
    print("start_time", start_time - timedelta(seconds=60))
    print("end_time", end_time + timedelta(seconds=60))
    print("invocations_sum:", str(invocations_sum))
    print("sum_duration:", str(sum_duration))
    print('lambda_duration_cost:', "${0:.8f}".format(lambda_duration_cost))
    print('lambda_invocation_cost:', "${0:.8f}".format(lambda_invocation_cost))
    print('lambda_total_cost:', "${0:.8f}".format(lambda_total_cost)) 
    print('')

    return lambda_total_cost

if __name__ == "__main__":

    # Check if the filter condition value is provided as a command line argument
    if len(sys.argv) < 2:
        print("Please provide the filter condition value as an argument.")
        sys.exit() 
    
    filter_value = sys.argv[1]  # Get the argument passed in the command line as the filter condition value
    filter_expr = f"_5 < {filter_value}"  # Use f-string to embed the input value into the filter condition
    
    main(filter_expr)