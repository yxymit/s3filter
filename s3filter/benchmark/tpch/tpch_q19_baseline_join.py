# -*- coding: utf-8 -*-
"""TPCH Q19 Baseline Benchmark

"""

import os

import numpy

import s3filter.util.constants
from s3filter import ROOT_DIR
from s3filter.op.aggregate import Aggregate
from s3filter.op.aggregate_expression import AggregateExpression
from s3filter.op.hash_join_build import HashJoinBuild
from s3filter.op.hash_join_probe import HashJoinProbe
from s3filter.op.join_expression import JoinExpression
from s3filter.op.map import Map
from s3filter.op.operator_connector import connect_many_to_many, connect_all_to_all, connect_many_to_one, \
    connect_one_to_one
from s3filter.plan.query_plan import QueryPlan
from s3filter.query import tpch_q19
from s3filter.util.test_util import gen_test_id


def main():
    if s3filter.util.constants.TPCH_SF == 10:
        run(parallel=True, use_pandas=False, secure=False, use_native=True, buffer_size=0, lineitem_parts=96,
            part_parts=4, lineitem_sharded=True, part_sharded=True)
    elif s3filter.util.constants.TPCH_SF == 1:
        # run(parallel=True, use_pandas=False, buffer_size=8192, lineitem_parts=1, part_parts=1)
        # run(parallel=True, use_pandas=False, buffer_size=8192, lineitem_parts=32, part_parts=4)
        run(parallel=True, use_pandas=True, secure=False, use_native=True, buffer_size=0, lineitem_parts=32,
            part_parts=4, lineitem_sharded=True, part_sharded=False)


def run(parallel, use_pandas, secure, use_native, buffer_size, lineitem_parts, part_parts, lineitem_sharded,
        part_sharded):
    """

    :return: None
    """

    secure = False
    use_native = False

    print('')
    print("TPCH Q19 Baseline Join")
    print("----------------------")

    query_plan = QueryPlan(is_async=parallel, buffer_size=buffer_size)

    # Define the operators
    lineitem_scan = map(lambda p:
                        query_plan.add_operator(
                            tpch_q19.sql_scan_lineitem_select_all_op(
                                lineitem_sharded,
                                p,
                                lineitem_parts,
                                use_pandas,
                                secure,
                                use_native,
                                'lineitem_scan' + '_' + str(p),
                                query_plan)),
                        range(0, lineitem_parts))

    part_scan = map(lambda p:
                    query_plan.add_operator(
                        tpch_q19.sql_scan_part_select_all_op(
                            part_sharded,
                            p,
                            part_parts,
                            use_pandas,
                            secure,
                            use_native,
                            'part_scan' + '_' + str(p), query_plan)),
                    range(0, part_parts))

    lineitem_project = map(lambda p:
                           query_plan.add_operator(
                               tpch_q19.project_partkey_quantity_extendedprice_discount_shipinstruct_shipmode_op(
                                   'lineitem_project' + '_' + str(p), query_plan)),
                           range(0, lineitem_parts))

    lineitem_map = map(lambda p:
                       query_plan.add_operator(Map('l_partkey', 'lineitem_map' + '_' + str(p), query_plan, False)),
                       range(0, part_parts))

    part_project = map(lambda p:
                       query_plan.add_operator(
                           tpch_q19.project_partkey_brand_size_container_op('part_project' + '_' + str(p),
                                                                            query_plan)),
                       range(0, part_parts))

    part_map = map(lambda p:
                   query_plan.add_operator(Map('p_partkey', 'part_map' + '_' + str(p), query_plan, False)),
                   range(0, part_parts))

    # lineitem_part_join = map(lambda p:
    #                          query_plan.add_operator(tpch_q19.join_op(query_plan)),
    #                          range(0, part_parts))

    lineitem_part_join_build = map(lambda p:
                                   query_plan.add_operator(
                                       HashJoinBuild('p_partkey',
                                                     'lineitem_partjoin_build' + '_' + str(p), query_plan,
                                                     False)),
                                   range(0, part_parts))

    lineitem_part_join_probe = map(lambda p:
                                   query_plan.add_operator(
                                       HashJoinProbe(JoinExpression('p_partkey', 'l_partkey'),
                                                     'lineitem_part_join_probe' + '_' + str(p),
                                                     query_plan, False)),
                                   range(0, part_parts))

    filter_op = map(lambda p:
                    query_plan.add_operator(tpch_q19.filter_def('filter_op' + '_' + str(p), query_plan)),
                    range(0, part_parts))
    aggregate = map(lambda p:
                    query_plan.add_operator(tpch_q19.aggregate_def('aggregate' + '_' + str(p), query_plan)),
                    range(0, part_parts))

    aggregate_reduce = query_plan.add_operator(
        Aggregate(
            [
                AggregateExpression(AggregateExpression.SUM, lambda t: float(t['_0']))
            ],
            'aggregate_reduce',
            query_plan,
            False))

    aggregate_project = query_plan.add_operator(
        tpch_q19.aggregate_project_def('aggregate_project', query_plan))
    collate = query_plan.add_operator(tpch_q19.collate_op('collate', query_plan))

    # Connect the operators
    # lineitem_scan.connect(lineitem_project)
    connect_many_to_many(lineitem_scan, lineitem_project)
    connect_all_to_all(lineitem_project, lineitem_map)

    # part_scan.connect(part_project)
    connect_many_to_many(part_scan, part_project)
    connect_many_to_many(part_project, part_map)

    # lineitem_part_join.connect_left_producer(lineitem_project)
    connect_all_to_all(part_map, lineitem_part_join_build)

    # lineitem_part_join.connect_right_producer(part_project)
    connect_many_to_many(lineitem_part_join_build, lineitem_part_join_probe)
    connect_many_to_many(lineitem_map, lineitem_part_join_probe)

    # lineitem_part_join.connect(filter_op)
    connect_many_to_many(lineitem_part_join_probe, filter_op)

    # filter_op.connect(aggregate)
    connect_many_to_many(filter_op, aggregate)

    # aggregate.connect(aggregate_project)
    connect_many_to_one(aggregate, aggregate_reduce)
    connect_one_to_one(aggregate_reduce, aggregate_project)

    # aggregate_project.connect(collate)
    connect_one_to_one(aggregate_project, collate)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../benchmark-output"), gen_test_id())

    # Start the query
    query_plan.execute()

    tuples = collate.tuples()

    collate.print_tuples(tuples)

    # Write the metrics
    query_plan.print_metrics()

    # Shut everything down
    query_plan.stop()

    field_names = ['revenue']

    assert len(tuples) == 1 + 1

    assert tuples[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    numpy.testing.assert_almost_equal(tuples[1], 3468861.097000001)


if __name__ == "__main__":
    main()
