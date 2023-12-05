# -*- coding: utf-8 -*-
"""Indexing Benchmark 

"""

import os

from s3filter import ROOT_DIR
from s3filter.op.collate import Collate
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.plan.query_plan import QueryPlan
from s3filter.sql.format import Format
from s3filter.util.test_util import gen_test_id
import sys


def main(price_value):
    run(True, True, 0, 10, 'access_method_benchmark/shards-1GB', Format.CSV, price_value)


def run(parallel, use_pandas, buffer_size, table_parts, path, format_, price_value):
    secure = False
    use_native = False
    print('')
    print("Indexing Benchmark")
    print("------------------")

    # Query plan
    query_plan = QueryPlan(is_async=parallel, buffer_size=buffer_size)

    # Scan Index Files
    scan = []
    for p in range(1, table_parts + 1):
        scan.append(query_plan.add_operator(
            SQLTableScan('{}/lineitem.{}.csv'.format(path, p),
                        "select L_ORDERKEY, L_EXTENDEDPRICE from S3Object "
                        "where cast(L_EXTENDEDPRICE as float) < {};".format(price_value), format_,
                        use_pandas, secure, use_native,
                        'scan_{}'.format(p), query_plan,
                        False)))

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

    # Shut everything down
    query_plan.stop()


if __name__ == "__main__":
    # Check if the filter condition value is provided as a command line argument
    if len(sys.argv) < 2:
        print("Please provide the filter condition value as an argument.")
        sys.exit() 

    main(float(sys.argv[1]))
