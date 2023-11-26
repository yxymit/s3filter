# -*- coding: utf-8 -*-
"""Indexing Benchmark 

"""

import os

from s3filter import ROOT_DIR
from s3filter.op.collate import Collate
from s3filter.op.sql_table_scan import SQLTableScanLambda
from s3filter.plan.query_plan import QueryPlan
from s3filter.sql.format import Format
from s3filter.util.test_util import gen_test_id
import boto3
from datetime import datetime, timedelta
import time


# define lambda function name and region
Lambda_Function_Name = 'demo_layers'
Lambda_Region_Name = 'us-east-2'

# lambda cost
# for 512GB memory, arm: <https://aws.amazon.com/lambda/pricing/>
COST_LAMBDA_DURATION_PER_SECOND = 0.0000067
COST_LAMBDA_REQUEST_PER_REQ = 0.0000002


def main():
    path = 'access_method_benchmark/shards-1GB'
    select_fields = "_0|_5"  # [l_orderkey, l_extendedprice]
    filter_expr =  "_5 < 2000"  # "_0 == '1'"
    start_part = 1
    table_parts = 2 
    run(parallel=True, 
        start_part=start_part, table_parts=table_parts, path=path, 
        select_fields=select_fields, filter_expr=filter_expr)


def run(parallel, start_part, table_parts, path, select_fields, filter_expr):
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
                        log_enabled=False))
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
    query_plan.execute()
    print('Done')
    tuples = collate.tuples()
    # collate.print_tuples(tuples)

    # Write the metrics
    query_plan.print_metrics()

    print("Lambda Cost")
    print("--------")
    print('lambda_duration_cost:', "${0:.8f}".format(lambda_duration_cost()))
    print('lambda_invocation_cost:', "${0:.8f}".format(lambda_invocation_cost()))

    # Shut everything down
    query_plan.stop()

def lambda_duration_cost():
    '''
    get duration from cloud watch, and calculate the actual cost
    '''
    retries = 0 
    max_retries = 10
    response = None

    while True:
        # Define start time and end time (last 5 minutes)
        start_time = datetime.utcnow() - timedelta(minutes=5) 
        end_time = datetime.utcnow()

        cloudwatch = boto3.client('cloudwatch', region_name= Lambda_Region_Name)
        response = cloudwatch.get_metric_statistics(
            Namespace='AWS/Lambda',
            MetricName='Duration',
            Dimensions=[{'Name': 'FunctionName', 'Value': Lambda_Function_Name}],
            StartTime=start_time,
            EndTime=end_time,
            Period=300,   # Period for statistics (300 seconds - 5 minutes)
            Statistics=['Maximum']  # Retrieve the maximum value
        )
        if len(response['Datapoints']) == 0:
            retries += 1
            if retries > max_retries:
                raise RuntimeError("Exceeded max retries")

            print("wait 30 seconds to make sure cloudWatch metrics have updated")
            time.sleep(30)
        else:
            break

    # sorted by timestamp
    sorted_datapoints = sorted(response['Datapoints'], key=lambda x: x['Timestamp'])
    # print(sorted_datapoints)
    latest_datapoint = sorted_datapoints[-1]
    # print(latest_datapoint)
    return latest_datapoint['Maximum'] / 1000 * COST_LAMBDA_DURATION_PER_SECOND

def lambda_invocation_cost():
    '''
    get invocations from cloud watch, and calculate the actual cost
    '''
    cloudwatch = boto3.client('cloudwatch', region_name=Lambda_Region_Name)

    # Define start time and end time (last 5 minutes)
    start_time = datetime.utcnow() - timedelta(minutes=5)
    end_time = datetime.utcnow()

    # Get 'Invocations' metric
    invocations_response = cloudwatch.get_metric_statistics(
        Namespace='AWS/Lambda',
        MetricName='Invocations',
        Dimensions=[{'Name': 'FunctionName', 'Value': Lambda_Function_Name}],
        StartTime=start_time,
        EndTime=end_time,
        Period=300,
        Statistics=['Sum']
    )

    invocations_sum = sum([datapoint['Sum'] for datapoint in invocations_response['Datapoints']])
    return invocations_sum * COST_LAMBDA_REQUEST_PER_REQ

if __name__ == "__main__":
    main()