# -*- coding: utf-8 -*-
"""Indexing Benchmark 

"""

import numpy as np
import pandas as pd

from s3filter.op.aggregate import Aggregate
from s3filter.op.collate import Collate
from s3filter.op.project import Project
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.plan.query_plan import QueryPlan
from s3filter.sql.format import Format


def main():
    path = 'parquet/tpch-sf10/lineitem_sharded1RG';
    #selected_cols = "l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity"
    #filter = "where l_orderkey <= 500"
    selected_cols = "*"
    filter = "where l_orderkey >= 0"
    run(True, True, 0, table_first_part=1, table_parts=96, selected_cols=selected_cols, filter=filter, path=path, format_=Format.PARQUET)


def run(parallel, use_pandas, buffer_size, table_first_part, table_parts, selected_cols, filter, path, format_):
    secure = False
    use_native = False
    print('')
    print("Parquet TableScan Benchmark")
    print("------------------")

    # Query plan
    query_plan = QueryPlan(is_async=parallel, buffer_size=buffer_size)

    # SQL scan the file
    scan = map(lambda p:
               query_plan.add_operator(
                   SQLTableScan("{}/lineitem.typed.1RowGroup.parquet.{}".format(path, p),
                                "select {} from S3Object {};".format(selected_cols, filter), format_,
                                use_pandas, secure, use_native,
                                'scan_{}'.format(p), query_plan,
                                False)),
               range(table_first_part, table_first_part + table_parts))

    # project
    def fn(df):
        df.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax',
                      'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct',
                      'l_shipmode', 'l_comment']
        #df.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity']
        df[['l_extendedprice']] = df[['l_extendedprice']].astype(np.float)
        return df

    #project = map(lambda p:
    #              query_plan.add_operator(
    #                  Project([], 'project_{}'.format(p), query_plan, False, fn)),
    #              range(table_first_part, table_first_part + table_parts))

    # aggregation
    #def agg_fun(df):
    #    return pd.DataFrame({'count': [len(df)]})

    #aggregate = query_plan.add_operator(
    #    Aggregate([], True, 'agg', query_plan, False, agg_fun))

    #collate = query_plan.add_operator(
    #    Collate('collate', query_plan, False))

    #map(lambda (p, o): o.connect(project[p]), enumerate(scan))
    #map(lambda (p, o): o.connect(aggregate), enumerate(project))
    #aggregate.connect(collate)

    # Plan settings
    print('')
    print("Settings")
    print("--------")
    print('')
    print('use_pandas: {}'.format(use_pandas))
    print("table parts: {}".format(table_parts))
    print('')

    # Write the plan graph
    # query_plan.write_graph(os.path.join(ROOT_DIR, "../benchmark-output"), gen_test_id() + "-" + str(table_parts))

    # Start the query
    query_plan.execute()
    print('Done')
    #tuples = collate.tuples()

    #collate.print_tuples(tuples)

    # Write the metrics
    query_plan.print_metrics()

    # Shut everything down
    query_plan.stop()


if __name__ == "__main__":
    main()
