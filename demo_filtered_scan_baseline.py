# -*- coding: utf-8 -*-
"""Filter query tests

"""
import os

from s3filter import ROOT_DIR
from s3filter.op.collate import Collate
from s3filter.op.filter import Filter
from s3filter.op.predicate_expression import PredicateExpression
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.plan.query_plan import QueryPlan
from s3filter.sql.format import Format
from s3filter.util.test_util import gen_test_id
import pandas as pd

def main():
    run(True, True, 0, 1, 2, 'access_method_benchmark/shards-1GB', Format.CSV)


def run(parallel, use_pandas, buffer_size, start_part, table_parts, path, format_):
    """
    Baseline of filter scan: fetch whole data to local server, then filter (all streaming pipeline)
    :return:
    """
    secure = False
    use_native = False

    print('')
    print("Scan Filter Baseline Benchmark")
    print("------------------")

    query_plan = QueryPlan(is_async=parallel, buffer_size=buffer_size)

    # Build Query plan
    scans = []
    for p in range(start_part, start_part + table_parts):
        scans.append(
            query_plan.add_operator(
                SQLTableScan(s3key='{}/lineitem.{}.csv'.format(path, p),
                            s3sql="select * from S3Object;",
                            format_=format_,
                            use_pandas=use_pandas,
                            secure=secure, use_native=use_native,
                            name='baseline_scan_{}'.format(p),
                            query_plan=query_plan,
                            log_enabled=False))
        )

    # filters
    def pd_expr(df):
        # expression for filtering condition
        return df['_0'] == '1'
    filters = []
    for p in range(start_part, start_part + table_parts):
        filters.append(
            query_plan.add_operator(
                Filter(expression=PredicateExpression(expr=None, pd_expr=pd_expr),
                        name='local_filter_{}'.format(p),
                        query_plan=query_plan,
                        log_enabled=False))
        )

    # collate
    collate = query_plan.add_operator(Collate('collate', query_plan, False))

    # connect operators
    for p, opt in enumerate(scans):
        opt.connect(filters[p])
    for p, opt in enumerate(filters):
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
    query_plan.write_graph(os.path.join(ROOT_DIR, "../benchmark-output-baseline"), gen_test_id() + "-" + str(table_parts))

    # Start the query
    query_plan.execute()
    print('Done')
    tuples = collate.tuples()
    collate.print_tuples(tuples)

    # Write the metrics
    query_plan.print_metrics()

    # Shut everything down
    query_plan.stop()


if __name__ == "__main__":
    main()