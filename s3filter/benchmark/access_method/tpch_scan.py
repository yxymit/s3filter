# -*- coding: utf-8 -*-
"""Indexing Benchmark 

"""

import os

import numpy as np
import pandas as pd

from s3filter import ROOT_DIR
from s3filter.op.aggregate import Aggregate
from s3filter.op.collate import Collate
from s3filter.op.filter import Filter, PredicateExpression
from s3filter.op.project import Project
from s3filter.op.table_scan import TableScan
from s3filter.plan.query_plan import QueryPlan
from s3filter.query.tpch import get_file_key
from s3filter.util.test_util import gen_test_id


def main():
    run(True, True, 0, 4, 0, 901.04, 10)


def run(parallel, use_pandas, buffer_size, table_parts, lower, upper, sf):
    secure = False
    use_native = False
    print('')
    print("Indexing Benchmark")
    print("------------------")

    # Query plan
    query_plan = QueryPlan(is_async=parallel, buffer_size=buffer_size)

    # scan the file
    scan = map(lambda p:
               query_plan.add_operator(
                   TableScan(get_file_key('lineitem', True, p, sf=sf),
                             use_pandas, secure, use_native,
                             'scan_{}'.format(p), query_plan,
                             False)),
               range(0, table_parts))

    # project
    def fn(df):
        df.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                      'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                      'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']
        df[['l_extendedprice']] = df[['l_extendedprice']].astype(np.float)
        return df

    project = map(lambda p:
                  query_plan.add_operator(
                      Project([], 'project_{}'.format(p), query_plan, False, fn)),
                  range(0, table_parts))

    # perform the filter locally
    def pd_expr(df):
        return (df['l_extendedprice'] >= lower) & (
                df['l_extendedprice'] <= upper)

    filters = map(lambda p:
                  query_plan.add_operator(
                      Filter(PredicateExpression(lambda t_:
                                                 (t_['l_extendedprice'] >= lower) and
                                                 (t_['l_extendedprice'] <= upper), pd_expr),
                             'filter_{}'.format(p), query_plan, False)),
                  range(0, table_parts))

    # aggregation
    def agg_fun(df):
        return pd.DataFrame({'count': [len(df)]})

    aggregate = query_plan.add_operator(
        Aggregate([], True, 'agg', query_plan, False, agg_fun))

    collate = query_plan.add_operator(
        Collate('collate', query_plan, False))

    map(lambda (p, o): o.connect(project[p]), enumerate(scan))
    map(lambda (p, o): o.connect(filters[p]), enumerate(project))
    map(lambda (p, o): o.connect(aggregate), enumerate(filters))
    aggregate.connect(collate)

    # scan[0].set_profiled(True, os.path.join(ROOT_DIR, "../benchmark-output/", gen_test_id() + "_scan_0" + ".prof"))
    # project[0].set_profiled(True, os.path.join(ROOT_DIR, "../benchmark-output/", gen_test_id() + "_profile_0" + ".prof"))
    # filters[0].set_profiled(True, os.path.join(ROOT_DIR, "../benchmark-output/", gen_test_id() + "_filters_0" + ".prof"))
    # aggregate.set_profiled(True, os.path.join(ROOT_DIR, "../benchmark-output/", gen_test_id() + "_aggregate" + ".prof"))
    # collate.set_profiled(True, os.path.join(ROOT_DIR, "../benchmark-output/", gen_test_id() + "_collate" + ".prof"))

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

    collate.print_tuples(tuples)

    # Write the metrics
    query_plan.print_metrics()

    # Shut everything down
    query_plan.stop()


if __name__ == "__main__":
    main()
