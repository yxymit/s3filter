# -*- coding: utf-8 -*-

from collections import OrderedDict

from s3filter.multiprocessing.worker_system import WorkerSystem
from s3filter.op.aggregate import Aggregate
from s3filter.op.aggregate_expression import AggregateExpression
from s3filter.op.collate import Collate
from s3filter.op.filter import Filter
from s3filter.op.hash_join_build import HashJoinBuild
from s3filter.op.hash_join_probe import HashJoinProbe
from s3filter.op.join_expression import JoinExpression
from s3filter.op.map import Map
from s3filter.op.operator_connector import connect_many_to_many, connect_all_to_all, connect_many_to_one, \
    connect_one_to_one
from s3filter.op.predicate_expression import PredicateExpression
from s3filter.op.project import ProjectExpression, Project
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.plan.query_plan import QueryPlan
from s3filter.query.join.synthetic_join_settings import SyntheticBaselineJoinSettings
from s3filter.query.tpch import get_file_key
from s3filter.query.tpch_q19 import get_sql_suffix
import numpy as np
import pandas as pd

def query_plan(settings):
    # type: (SyntheticBaselineJoinSettings) -> QueryPlan
    """

    :type settings:
    :return: None
    """

    if settings.use_shared_mem:
        system = WorkerSystem(settings.shared_memory_size)
    else:
        system = None

    query_plan = QueryPlan(system, is_async=settings.parallel, buffer_size=settings.buffer_size,
                           use_shared_mem=settings.use_shared_mem)

    # Define the operators
    scan_A = \
        map(lambda p:
            query_plan.add_operator(
                SQLTableScan(get_file_key(settings.table_A_key, settings.table_A_sharded, p, settings.sf),
                             "select "
                             "  * "
                             "from "
                             "  S3Object "
                             "{}"
                             .format(
                                 get_sql_suffix(settings.table_A_key, settings.table_A_parts, p,
                                                settings.table_A_sharded, add_where=True)), settings.format_,
                             settings.use_pandas,
                             settings.secure,
                             settings.use_native,
                             'scan_A_{}'.format(p),
                             query_plan,
                             False)),
            range(0, settings.table_A_parts))

    field_names_map_A = OrderedDict(
        zip(['_{}'.format(i) for i, name in enumerate(settings.table_A_field_names)], settings.table_A_field_names))

    def project_fn_A(df):
        df = df.rename(columns=field_names_map_A, copy=False)
        return df

    project_A = map(lambda p:
                    query_plan.add_operator(Project(
                        [ProjectExpression(k, v) for k, v in field_names_map_A.iteritems()],
                        'project_A_{}'.format(p),
                        query_plan,
                        False,
                        project_fn_A)),
                    range(0, settings.table_A_parts))

    if settings.table_A_filter_fn is not None:
        filter_A = map(lambda p:
                       query_plan.add_operator(Filter(
                           PredicateExpression(None, pd_expr=settings.table_A_filter_fn), 'filter_A_{}'.format(p), query_plan,
                           False)),
                       range(0, settings.table_A_parts))

    scan_B = \
        map(lambda p:
            query_plan.add_operator(
                SQLTableScan(get_file_key(settings.table_B_key, settings.table_B_sharded, p, settings.sf),
                             "select "
                             "  * "
                             "from "
                             "  S3Object "
                             "{}"
                             .format(
                                 get_sql_suffix(settings.table_B_key, settings.table_B_parts, p,
                                                settings.table_B_sharded, add_where=True)), settings.format_,
                             settings.use_pandas,
                             settings.secure,
                             settings.use_native,
                             'scan_B_{}'.format(p),
                             query_plan,
                             False)),
            range(0, settings.table_B_parts))

    field_names_map_B = OrderedDict(
        zip(['_{}'.format(i) for i, name in enumerate(settings.table_B_field_names)], settings.table_B_field_names))

    def project_fn_B(df):
        df.rename(columns=field_names_map_B, inplace=True)
        return df

    project_B = map(lambda p:
                    query_plan.add_operator(Project(
                        [ProjectExpression(k, v) for k, v in field_names_map_B.iteritems()],
                        'project_B_{}'.format(p),
                        query_plan,
                        False,
                        project_fn_B)),
                    range(0, settings.table_B_parts))

    if settings.table_B_filter_fn is not None:
        filter_b = map(lambda p:
                       query_plan.add_operator(Filter(
                           PredicateExpression(None, pd_expr=settings.table_B_filter_fn), 'filter_b' + '_{}'.format(p), query_plan,
                           False)),
                       range(0, settings.table_B_parts))

    if settings.table_C_key is not None:
        scan_C = \
            map(lambda p:
                query_plan.add_operator(
                    SQLTableScan(get_file_key(settings.table_C_key, settings.table_C_sharded, p, settings.sf),
                                 "select "
                                 "  * "
                                 "from "
                                 "  S3Object "
                                 "{}"
                                 .format(
                                     get_sql_suffix(settings.table_C_key, settings.table_C_parts, p,
                                                    settings.table_C_sharded, add_where=True)), settings.format_,
                                 settings.use_pandas,
                                 settings.secure,
                                 settings.use_native,
                                 'scan_C_{}'.format(p),
                                 query_plan,
                                 False)),
                range(0, settings.table_C_parts))

        field_names_map_C = OrderedDict(
            zip(['_{}'.format(i) for i, name in enumerate(settings.table_C_field_names)], settings.table_C_field_names))

        def project_fn_C(df):
            df = df.rename(columns=field_names_map_C, copy=False)
            return df

        project_C = map(lambda p:
                        query_plan.add_operator(Project(
                            [ProjectExpression(k, v) for k, v in field_names_map_C.iteritems()],
                            'project_C_{}'.format(p),
                            query_plan,
                            False,
                            project_fn_C)),
                        range(0, settings.table_C_parts))

        filter_c = map(lambda p:
                       query_plan.add_operator(Filter(
                           PredicateExpression(None, pd_expr=settings.table_C_filter_fn), 'filter_c' + '_{}'.format(p), query_plan,
                           False)),
                       range(0, settings.table_C_parts))

        map_B_to_C = map(lambda p:
                         query_plan.add_operator(
                             Map(settings.table_B_BC_join_key, 'map_B_to_C_{}'.format(p), query_plan, False)),
                         range(0, settings.table_B_parts))

        map_C_to_C = map(lambda p:
                         query_plan.add_operator(
                             Map(settings.table_C_BC_join_key, 'map_C_to_C_{}'.format(p), query_plan, False)),
                         range(0, settings.table_C_parts))

        join_build_AB_C = map(lambda p:
                              query_plan.add_operator(
                                  HashJoinBuild(settings.table_B_BC_join_key, 'join_build_AB_C_{}'.format(p),
                                                query_plan,
                                                False)),
                              range(0, settings.table_C_parts))

        join_probe_AB_C = map(lambda p:
                              query_plan.add_operator(
                                  HashJoinProbe(
                                      JoinExpression(settings.table_B_BC_join_key, settings.table_C_BC_join_key),
                                      'join_probe_AB_C_{}'.format(p),
                                      query_plan, False)),
                              range(0, settings.table_C_parts))

    map_A_to_B = map(lambda p:
                     query_plan.add_operator(
                         Map(settings.table_A_AB_join_key, 'map_A_to_B_{}'.format(p), query_plan, False)),
                     range(0, settings.table_A_parts))

    map_B_to_B = map(lambda p:
                     query_plan.add_operator(
                         Map(settings.table_B_AB_join_key, 'map_B_to_B_{}'.format(p), query_plan, False)),
                     range(0, settings.table_B_parts))

    join_build_A_B = map(lambda p:
                         query_plan.add_operator(
                             HashJoinBuild(settings.table_A_AB_join_key, 'join_build_A_B_{}'.format(p), query_plan,
                                           False)),
                         range(0, settings.other_parts))

    join_probe_A_B = map(lambda p:
                         query_plan.add_operator(
                             HashJoinProbe(JoinExpression(settings.table_A_AB_join_key, settings.table_B_AB_join_key),
                                           'join_probe_A_B_{}'.format(p),
                                           query_plan, False)),
                         range(0, settings.other_parts))

    if settings.table_C_key is None:

        def part_aggregate_fn(df):
            sum_ = df[settings.table_B_detail_field_name].astype(np.float).sum()
            return pd.DataFrame({'_0': [sum_]})

        part_aggregate = map(lambda p:
                             query_plan.add_operator(Aggregate(
                                 [
                                     AggregateExpression(AggregateExpression.SUM,
                                                         lambda t: float(t[settings.table_B_detail_field_name]))
                                 ],
                                 settings.use_pandas,
                                 'part_aggregate_{}'.format(p), query_plan, False, part_aggregate_fn)),
                             range(0, settings.other_parts))

    else:
        def part_aggregate_fn(df):
            sum_ = df[settings.table_C_detail_field_name].astype(np.float).sum()
            return pd.DataFrame({'_0': [sum_]})

        part_aggregate = map(lambda p:
                             query_plan.add_operator(Aggregate(
                                 [
                                     AggregateExpression(AggregateExpression.SUM,
                                                         lambda t: float(t[settings.table_C_detail_field_name]))
                                 ],
                                 settings.use_pandas,
                                 'part_aggregate_{}'.format(p), query_plan, False, part_aggregate_fn)),
                             range(0, settings.table_C_parts))

    def aggregate_reduce_fn(df):
        sum_ = df['_0'].astype(np.float).sum()
        return pd.DataFrame({'_0': [sum_]})

    aggregate_reduce = query_plan.add_operator(Aggregate(
        [
            AggregateExpression(AggregateExpression.SUM, lambda t: float(t['_0']))
        ],
        settings.use_pandas,
        'aggregate_reduce', query_plan, False, aggregate_reduce_fn))

    aggregate_project = query_plan.add_operator(Project(
        [
            ProjectExpression(lambda t: t['_0'], 'total_balance')
        ],
        'aggregate_project', query_plan,
        False))

    collate = query_plan.add_operator(Collate('collate', query_plan, False))

    # Inline some of the operators
    map(lambda o: o.set_async(False), project_A)
    if settings.table_A_filter_fn is not None:
        map(lambda o: o.set_async(False), filter_A)
    map(lambda o: o.set_async(False), project_B)
    if settings.table_B_filter_fn is not None:
        map(lambda o: o.set_async(False), filter_b)
    map(lambda o: o.set_async(False), map_A_to_B)
    map(lambda o: o.set_async(False), map_B_to_B)
    if settings.table_C_key is not None:
        map(lambda o: o.set_async(False), map_B_to_C)
        map(lambda o: o.set_async(False), map_C_to_C)
        map(lambda o: o.set_async(False), project_C)
        map(lambda o: o.set_async(False), filter_c)
    map(lambda o: o.set_async(False), part_aggregate)
    aggregate_project.set_async(False)

    # Connect the operators
    connect_many_to_many(scan_A, project_A)
    if settings.table_A_filter_fn is not None:
        connect_many_to_many(project_A, filter_A)
        connect_many_to_many(filter_A, map_A_to_B)
    else:
        connect_many_to_many(project_A, map_A_to_B)
    connect_all_to_all(map_A_to_B, join_build_A_B)
    connect_many_to_many(join_build_A_B, join_probe_A_B)

    connect_many_to_many(scan_B, project_B)
    if settings.table_B_filter_fn is not None:
        connect_many_to_many(project_B, filter_b)
        connect_many_to_many(filter_b, map_B_to_B)
    else:
        connect_many_to_many(project_B, map_B_to_B)

    connect_all_to_all(map_B_to_B, join_probe_A_B)

    if settings.table_C_key is None:
        connect_many_to_many(join_probe_A_B, part_aggregate)
    else:
        connect_many_to_many(join_probe_A_B, map_B_to_C)
        connect_all_to_all(map_B_to_C, join_build_AB_C)
        connect_many_to_many(join_build_AB_C, join_probe_AB_C)
        connect_many_to_many(scan_C, project_C)
        connect_many_to_many(project_C, filter_c)
        connect_many_to_many(filter_c, map_C_to_C)
        connect_all_to_all(map_C_to_C, join_probe_AB_C)
        connect_many_to_many(join_probe_AB_C, part_aggregate)

    connect_many_to_one(part_aggregate, aggregate_reduce)
    connect_one_to_one(aggregate_reduce, aggregate_project)
    connect_one_to_one(aggregate_project, collate)

    return query_plan
