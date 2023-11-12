"""Top K baseline

"""
import os

import numpy as np

from s3filter import ROOT_DIR
from s3filter.op.collate import Collate
from s3filter.op.project import Project, ProjectExpression
from s3filter.op.sort import SortExpression
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.op.top import Top
from s3filter.op.top_filter_build import TopKFilterBuild
from s3filter.plan.query_plan import QueryPlan
from s3filter.sql.format import Format
from s3filter.util.test_util import gen_test_id


def main():
    path = 'access_method_benchmark/shards-1GB'
    k = 100
    queried_columns = ['l_orderkey', 'l_extendedprice']
    select_columns = ", ".join(queried_columns)
    if len(queried_columns) == 16:
        select_columns = "*"

    run('l_extendedprice', k, sample_size=5000, parallel=True, use_pandas=True,
        sort_order='ASC', buffer_size=0, table_first_part=1, queried_columns=queried_columns,
        select_columns=select_columns, table_parts=2, path=path, format_= Format.CSV)

def run(sort_field, k, sample_size, parallel, use_pandas, sort_order, buffer_size, table_first_part, table_parts,
         queried_columns, select_columns, path, format_):
    """
    Executes the baseline topk query by scanning a table and keeping track of the max/min records in a heap
    :return:
    """

    secure = False
    use_native = False
    print('')
    print("Top K Benchmark, Sampling. Sort Field: {}, Order: {}".format(sort_field, sort_order))
    print("----------------------")

    # Query plan
    query_plan = QueryPlan(is_async=parallel, buffer_size=buffer_size)
   
    # Sampling 
    per_part_samples = int(sample_size / table_parts)
    # sample_scan = map(lambda p:
    #                   query_plan.add_operator(
    #                     SQLTableScan("{}/lineitem.{}.csv".format(path, p),
    #                         'select {} from S3Object limit {};'.format(sort_field, per_part_samples), format_,
    #                         use_pandas, secure, use_native, 
    #                         'sample_scan_{}'.format(p), query_plan, False)),
    #                   range(table_first_part, table_first_part + table_parts))
    sample_scan = []
    for p in range(table_first_part, table_first_part + table_parts):
        sample_scan.append(
            query_plan.add_operator(
                SQLTableScan("{}/lineitem.{}.csv".format(path, p),
                    'select {} from S3Object limit {};'.format(sort_field, per_part_samples), format_,
                    use_pandas, secure, use_native, 
                    'sample_scan_{}'.format(p), query_plan, False)
                )
        )
    # Sampling project
    def project_fn1(df):
        df.columns = [sort_field]
        df[ [sort_field] ] = df[ [sort_field] ].astype(np.float)
        return df
   
    project_exprs = [ProjectExpression(lambda t_: t_['_0'], sort_field)] 
    
    # sample_project = map(lambda p: 
    #               query_plan.add_operator( 
    #                   Project(project_exprs, 'sample_project_{}'.format(p), query_plan, False, project_fn1)),
    #               range(table_first_part, table_first_part + table_parts))
    sample_project = []
    for p in range(table_first_part, table_first_part + table_parts):
        sample_project.append(
            query_plan.add_operator( 
                Project(project_exprs, 'sample_project_{}'.format(p), query_plan, False, project_fn1))
        )

    # TopK samples
    sort_expr = SortExpression(sort_field, float, sort_order)
    sample_topk = query_plan.add_operator(
                    Top(k, sort_expr, use_pandas, 'sample_topk', query_plan, False)) 

    # Generate SQL command for second scan 
    sql_gen = query_plan.add_operator(
                   TopKFilterBuild( sort_order, 'float', 'select {} from S3object '.format(select_columns), 
                                    ' CAST({} as float) '.format(sort_field), 'sql_gen', query_plan, False ))
    
    # Scan
    # scan = map(lambda p: 
    #            query_plan.add_operator(
    #                 SQLTableScan("{}/lineitem.{}.csv".format(path, p),
    #                     "", format_, use_pandas, secure, use_native,
    #                     'scan_{}'.format(p), query_plan,
    #                     False)),
    #            range(table_first_part, table_first_part + table_parts))
    scan = []
    for p in range(table_first_part, table_first_part + table_parts):
        scan.append(
            query_plan.add_operator(
                SQLTableScan("{}/lineitem.{}.csv".format(path, p),
                    "", format_, use_pandas, secure, use_native,
                    'scan_{}'.format(p), query_plan, False))
        )
 
    # Project
    def project_fn2(df):
        df.columns = queried_columns
        df[ [sort_field] ] = df[ [sort_field] ].astype(np.float)
        return df
   
    project_exprs = [ProjectExpression(lambda t_: t_['_0'], sort_field)] 
    
    # project = map(lambda p: 
    #               query_plan.add_operator( 
    #                   Project(project_exprs, 'project_{}'.format(p), query_plan, False, project_fn2)),
    #               range(table_first_part, table_first_part + table_parts))
    project = []
    for p in range(table_first_part, table_first_part + table_parts):
        project.append(
            query_plan.add_operator( 
                Project(project_exprs, 'project_{}'.format(p), query_plan, False, project_fn2))
        )

    # TopK
    # topk = map(lambda p: 
    #            query_plan.add_operator(
    #                 Top(k, sort_expr, use_pandas, 'topk_{}'.format(p), query_plan, False)),
    #            range(table_first_part, table_first_part + table_parts))
    topk = []
    for p in range(table_first_part, table_first_part + table_parts):
        topk.append(
            query_plan.add_operator(
                Top(k, sort_expr, use_pandas, 'topk_{}'.format(p), query_plan, False))
        )

    # TopK reduce
    topk_reduce = query_plan.add_operator(
                    Top(k, sort_expr, use_pandas, 'topk_reduce', query_plan, False)) 

    collate = query_plan.add_operator(
        Collate('collate', query_plan, False))

    #profile_path = '../benchmark-output/groupby/'
    #scan[0].set_profiled(True, os.path.join(ROOT_DIR, profile_path, gen_test_id() + "_scan_0" + ".prof"))
    #project[0].set_profiled(True, os.path.join(ROOT_DIR, profile_path, gen_test_id() + "_project_0" + ".prof"))
    #groupby[0].set_profiled(True, os.path.join(ROOT_DIR, profile_path, gen_test_id() + "_groupby_0" + ".prof"))
    #groupby_reduce.set_profiled(True, os.path.join(ROOT_DIR, profile_path, gen_test_id() + "_groupby_reduce" + ".prof"))
    #collate.set_profiled(True, os.path.join(ROOT_DIR, profile_path, gen_test_id() + "_collate" + ".prof"))
    
    for p, o in enumerate(sample_scan):
        o.connect(sample_project[p])
        sample_project[p].connect(sample_topk)
    sample_topk.connect(sql_gen)

    for p, o in enumerate(scan):
        sql_gen.connect(o)
        o.connect(project[p])
        project[p].connect(topk[p])
        topk[p].connect(topk_reduce)
    topk_reduce.connect(collate)

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
