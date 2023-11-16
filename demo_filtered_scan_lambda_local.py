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


def main():
    path = 'access_method_benchmark/shards-1GB'
    select_fields = "_0|_5"  # [l_orderkey, l_extendedprice]
    filter_expr =  "_0 == '1'"  # "l_orderkey == '1'"
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

    # Shut everything down
    query_plan.stop()


if __name__ == "__main__":
    main()