# -*- coding: utf-8 -*-
"""Join experiments on TPC-H query 17

These queries test the performance of several approaches to running the TCP-H Q17 query.

TPCH Query 19

    select
        sum(l_extendedprice* (1 - l_discount)) as revenue
    from
        lineitem,
        part
    where
    (
        p_partkey = l_partkey
        and p_brand = cast('Brand#11' as char(10))
        and p_container in (
            cast('SM CASE' as char(10)),
            cast('SM BOX' as char(10)),
            cast('SM PACK' as char(10)),
            cast('SM PKG' as char(10))
        )
        and l_quantity >= 3 and l_quantity <= 3 + 10
        and p_size between 1 and 5
        and l_shipmode in (cast('AIR' as char(10)), cast('AIR REG' as char(10)))
        and l_shipinstruct = cast('DELIVER IN PERSON' as char(25))
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = cast('Brand#44' as char(10))
        and p_container in (
            cast('MED BAG' as char(10)),
            cast('MED BOX' as char(10)),
            cast('MED PKG' as char(10)),
            cast('MED PACK' as char(10))
        )
        and l_quantity >= 16 and l_quantity <= 16 + 10
        and p_size between 1 and 10
        and l_shipmode in (cast('AIR' as char(10)), cast('AIR REG' as char(10)))
        and l_shipinstruct = cast('DELIVER IN PERSON' as char(25))
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = cast('Brand#53' as char(10))
        and p_container in (
            cast('LG CASE' as char(10)),
            cast('LG BOX' as char(10)),
            cast('LG PACK' as char(10)),
            cast('LG PKG' as char(10))
        )
        and l_quantity >= 24 and l_quantity <= 24 + 10
        and p_size between 1 and 15
        and l_shipmode in (cast('AIR' as char(10)), cast('AIR REG' as char(10)))
        and l_shipinstruct = cast('DELIVER IN PERSON' as char(25))
    );





"""
from op.bloom_create import BloomCreate
from op.sql_table_scan_bloom_use import SQLTableScanBloomUse
from plan.query_plan import QueryPlan
from op.aggregate import Aggregate
from op.aggregate_expression import AggregateExpression
from op.collate import Collate
from op.filter import Filter
from op.join import Join, JoinExpression
from op.predicate_expression import PredicateExpression
from op.project import Project, ProjectExpr
from op.sql_table_scan import SQLTableScan
from sql.function import sum_fn
from util.test_util import gen_test_id


def collate_op():
    return Collate('collate', False)


def aggregate_def():
    return Aggregate(
        [
            AggregateExpression(lambda t_, ctx: sum_fn(float(t_['l_extendedprice']) *
                                                       float((1 - float(t_['l_discount']))),
                                                       ctx))
        ],
        'aggregate',
        False)


def join_op():
    return Join(JoinExpression('l_partkey', 'p_partkey'), 'lineitem_part_join', False)


def aggregate_project_def():
    return Project(
        [
            ProjectExpr(lambda t_: t_['_0'], 'revenue')
        ],
        'aggregate_project',
        False)


def filter_def():
    return Filter(PredicateExpression(lambda t_:
                                      (
                                              t_['p_brand'] == 'Brand#11' and
                                              t_['p_container'] in ['SM CASE', 'SM BOX', 'SM PACK',
                                                                    'SM PKG'] and
                                              3 <= int(t_['l_quantity']) <= 3 + 10 and
                                              1 < int(t_['p_size']) < 5 and
                                              t_['l_shipmode'] in ['AIR', 'AIR REG'] and
                                              t_['l_shipinstruct'] == 'DELIVER IN PERSON'
                                      ) or (
                                              t_['p_brand'] == 'Brand#44' and
                                              t_['p_container'] in ['MED BAG', 'MED BOX', 'MED PACK',
                                                                    'MED PKG'] and
                                              16 <= int(t_['l_quantity']) <= 16 + 10 and
                                              1 < int(t_['p_size']) < 10 and
                                              t_['l_shipmode'] in ['AIR', 'AIR REG'] and
                                              t_['l_shipinstruct'] == 'DELIVER IN PERSON'
                                      ) or (
                                              t_['p_brand'] == 'Brand#53' and
                                              t_['p_container'] in ['LG BAG', 'LG BOX', 'LG PACK',
                                                                    'LG PKG'] and
                                              24 <= int(t_['l_quantity']) <= 24 + 10 and
                                              1 < int(t_['p_size']) < 15 and
                                              t_['l_shipmode'] in ['AIR', 'AIR REG'] and
                                              t_['l_shipinstruct'] == 'DELIVER IN PERSON'
                                      )),
                  'filter',
                  False)


def test_join_baseline():
    """

    :return: None
    """

    query_plan = QueryPlan("TPCH Q19 Baseline Join Test")

    # Define the operators

    # with lineitem_scan as (select * from lineitem)
    lineitem_scan = query_plan.add_operator(SQLTableScan('lineitem.csv',
                                                         "select "
                                                         "  * "
                                                         "from "
                                                         "  S3Object "
                                                         "where "
                                                         "  l_partkey = '103853' or "
                                                         "  l_partkey = '104277' or "
                                                         "  l_partkey = '104744' ",
                                                         'lineitem_scan',
                                                         False))

    # with part_scan as (select * from part)
    part_scan = query_plan.add_operator(SQLTableScan('part.csv',
                                                     "select "
                                                     "  * "
                                                     "from "
                                                     "  S3Object "
                                                     "where "
                                                     "  p_partkey = '103853' or "
                                                     "  p_partkey = '104277' or "
                                                     "  p_partkey = '104744' ",
                                                     'part_scan',
                                                     False))

    # with lineitem_project as (
    # select
    # _1 as l_partkey, _4 as l_quantity,_5 as l_quantity, 6 as l_quantity, 13 as l_quantity, 14 as l_quantity
    # from lineitem_scan
    # )
    lineitem_project = query_plan.add_operator(Project(
        [
            ProjectExpr(lambda t_: t_['_1'], 'l_partkey'),
            ProjectExpr(lambda t_: t_['_4'], 'l_quantity'),
            ProjectExpr(lambda t_: t_['_5'], 'l_extendedprice'),
            ProjectExpr(lambda t_: t_['_6'], 'l_discount'),
            ProjectExpr(lambda t_: t_['_13'], 'l_shipinstruct'),
            ProjectExpr(lambda t_: t_['_14'], 'l_shipmode')
        ],
        'lineitem_project',
        False))

    # with part_project as (
    # select
    # _0 as p_partkey, _3 as p_brand, _5 as p_size, _6 as p_container
    # from part_scan
    # )
    part_project = query_plan.add_operator(Project(
        [
            ProjectExpr(lambda t_: t_['_0'], 'p_partkey'),
            ProjectExpr(lambda t_: t_['_3'], 'p_brand'),
            ProjectExpr(lambda t_: t_['_5'], 'p_size'),
            ProjectExpr(lambda t_: t_['_6'], 'p_container')
        ],
        'part_project',
        False))

    lineitem_part_join = query_plan.add_operator(join_op())
    filter_op = query_plan.add_operator(filter_def())
    aggregate = query_plan.add_operator(aggregate_def())
    aggregate_project = query_plan.add_operator(aggregate_project_def())
    collate = query_plan.add_operator(collate_op())

    # Connect the operators
    lineitem_scan.connect(lineitem_project)
    part_scan.connect(part_project)

    lineitem_part_join.connect_left_producer(lineitem_project)
    lineitem_part_join.connect_right_producer(part_project)

    lineitem_part_join.connect(filter_op)
    filter_op.connect(aggregate)
    aggregate.connect(aggregate_project)
    aggregate_project.connect(collate)

    # filter_op.connect(collate)

    # Write the plan graph
    query_plan.write_graph(gen_test_id())

    # Start the query
    part_scan.start()
    lineitem_scan.start()

    # Assert the results
    num_rows = 0
    for t in collate.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    field_names = ['revenue']

    assert len(collate.tuples()) == 1 + 1

    assert collate.tuples()[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    assert collate.tuples()[1] == [92403.0667]

    # Write the metrics
    query_plan.print_metrics()


def test_join_filtered():
    """

    :return: None
    """

    query_plan = QueryPlan("TPCH Q19 Filtered Join Test")

    # Define the operators

    # with lineitem_scan as (select * from lineitem)
    lineitem_scan = query_plan.add_operator(SQLTableScan('lineitem.csv',
                                                         "select "
                                                         "  l_partkey, "
                                                         "  l_quantity, "
                                                         "  l_extendedprice, "
                                                         "  l_discount, "
                                                         "  l_shipinstruct, "
                                                         "  l_shipmode "
                                                         "from "
                                                         "  S3Object "
                                                         "where "
                                                         "  l_partkey = '103853' or "
                                                         "  l_partkey = '104277' or "
                                                         "  l_partkey = '104744' ",
                                                         'lineitem_scan',
                                                         False))

    # with part_scan as (select * from part)
    part_scan = query_plan.add_operator(SQLTableScan('part.csv',
                                                     "select "
                                                     "  p_partkey, "
                                                     "  p_brand, "
                                                     "  p_size, "
                                                     "  p_container "
                                                     "from "
                                                     "  S3Object "
                                                     "where "
                                                     "  p_partkey = '103853' or "
                                                     "  p_partkey = '104277' or "
                                                     "  p_partkey = '104744' ",
                                                     'part_scan',
                                                     False))

    # with lineitem_project as (
    # select
    # _1 as l_partkey, _4 as l_quantity,_5 as l_quantity, 6 as l_quantity, 13 as l_quantity, 14 as l_quantity
    # from lineitem_scan
    # )
    lineitem_project = query_plan.add_operator(Project(
        [
            ProjectExpr(lambda t_: t_['_0'], 'l_partkey'),
            ProjectExpr(lambda t_: t_['_1'], 'l_quantity'),
            ProjectExpr(lambda t_: t_['_2'], 'l_extendedprice'),
            ProjectExpr(lambda t_: t_['_3'], 'l_discount'),
            ProjectExpr(lambda t_: t_['_4'], 'l_shipinstruct'),
            ProjectExpr(lambda t_: t_['_5'], 'l_shipmode')
        ],
        'lineitem_project',
        False))

    # with part_project as (
    # select
    # _0 as p_partkey, _3 as p_brand, _5 as p_size, _6 as p_container
    # from part_scan
    # )
    part_project = query_plan.add_operator(Project(
        [
            ProjectExpr(lambda t_: t_['_0'], 'p_partkey'),
            ProjectExpr(lambda t_: t_['_1'], 'p_brand'),
            ProjectExpr(lambda t_: t_['_2'], 'p_size'),
            ProjectExpr(lambda t_: t_['_3'], 'p_container')
        ],
        'part_project',
        False))

    lineitem_part_join = query_plan.add_operator(join_op())
    filter_op = query_plan.add_operator(filter_def())
    aggregate = query_plan.add_operator(aggregate_def())
    aggregate_project = query_plan.add_operator(aggregate_project_def())
    collate = query_plan.add_operator(collate_op())

    # Connect the operators
    lineitem_scan.connect(lineitem_project)
    part_scan.connect(part_project)

    lineitem_part_join.connect_left_producer(lineitem_project)
    lineitem_part_join.connect_right_producer(part_project)

    lineitem_part_join.connect(filter_op)
    filter_op.connect(aggregate)
    aggregate.connect(aggregate_project)
    aggregate_project.connect(collate)

    # Write the plan graph
    query_plan.write_graph(gen_test_id())

    # Start the query
    part_scan.start()
    lineitem_scan.start()

    # Assert the results
    num_rows = 0
    for t in collate.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    field_names = ['revenue']

    assert len(collate.tuples()) == 1 + 1

    assert collate.tuples()[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    assert collate.tuples()[1] == [92403.0667]

    # Write the metrics
    query_plan.print_metrics()


def test_join_bloom():
    """

    :return: None
    """

    query_plan = QueryPlan("TPCH Q19 Bloom Join Test")

    # Define the operators

    # with part_scan as (select * from part)
    part_scan = query_plan.add_operator(SQLTableScan('part.csv',
                                                     "select "
                                                     "  p_partkey, "
                                                     "  p_brand, "
                                                     "  p_size, "
                                                     "  p_container "
                                                     "from "
                                                     "  S3Object "
                                                     "where "
                                                     "  p_partkey = '103853' or "
                                                     "  p_partkey = '104277' or "
                                                     "  p_partkey = '104744' and "
                                                     "  ( "
                                                     "      ( "
                                                     "          p_brand = 'Brand#11' "
                                                     "          and p_container in ("
                                                     "              'SM CASE', "
                                                     "              'SM BOX', "
                                                     "              'SM PACK', "
                                                     "              'SM PKG'"
                                                     "          ) "
                                                     "          and cast(p_size as integer) between 1 and 5 "
                                                     "      ) "
                                                     "      or "
                                                     "      ( "
                                                     "          p_brand = 'Brand#44' "
                                                     "          and p_container in ("
                                                     "              'MED BAG', "
                                                     "              'MED BOX', "
                                                     "              'MED PKG', "
                                                     "              'MED PACK'"
                                                     "          ) "
                                                     "          and cast(p_size as integer) between 1 and 10 "
                                                     "      ) "
                                                     "      or "
                                                     "      ( "
                                                     "          p_brand = 'Brand#53' "
                                                     "          and p_container in ("
                                                     "              'LG CASE', "
                                                     "              'LG BOX', "
                                                     "              'LG PACK', "
                                                     "              'LG PKG'"
                                                     "          ) "
                                                     "          and cast(p_size as integer) between 1 and 15 "
                                                     "      ) "
                                                     "  ) ",
                                                     'part_scan',
                                                     False))

    # with lineitem_project as (
    # select
    # _1 as l_partkey, _4 as l_quantity,_5 as l_quantity, 6 as l_quantity, 13 as l_quantity, 14 as l_quantity
    # from lineitem_scan
    # )
    lineitem_project = query_plan.add_operator(Project(
        [
            ProjectExpr(lambda t_: t_['_0'], 'l_partkey'),
            ProjectExpr(lambda t_: t_['_1'], 'l_quantity'),
            ProjectExpr(lambda t_: t_['_2'], 'l_extendedprice'),
            ProjectExpr(lambda t_: t_['_3'], 'l_discount'),
            ProjectExpr(lambda t_: t_['_4'], 'l_shipinstruct'),
            ProjectExpr(lambda t_: t_['_5'], 'l_shipmode')
        ],
        'lineitem_project',
        False))

    # with part_project as (
    # select
    # _0 as p_partkey, _3 as p_brand, _5 as p_size, _6 as p_container
    # from part_scan
    # )
    part_project = query_plan.add_operator(Project(
        [
            ProjectExpr(lambda t_: t_['_0'], 'p_partkey'),
            ProjectExpr(lambda t_: t_['_1'], 'p_brand'),
            ProjectExpr(lambda t_: t_['_2'], 'p_size'),
            ProjectExpr(lambda t_: t_['_3'], 'p_container')
        ],
        'part_project',
        False))

    part_bloom_create = query_plan.add_operator(
        BloomCreate('p_partkey', 'part_bloom_create', False))

    lineitem_bloom_use = query_plan.add_operator(
        SQLTableScanBloomUse('lineitem.csv',
                             "select "
                             "  l_partkey, "
                             "  l_quantity, "
                             "  l_extendedprice, "
                             "  l_discount, "
                             "  l_shipinstruct, "
                             "  l_shipmode "
                             "from "
                             "  S3Object "
                             "where "
                             "  l_partkey = '103853' or "
                             "  l_partkey = '104277' or "
                             "  l_partkey = '104744' and "
                             "  ( "
                             "      ( "
                             "          cast(l_quantity as integer) >= 3 and cast(l_quantity as integer) <= 3 + 10 "
                             "          and l_shipmode in ('AIR', 'AIR REG') "
                             "          and l_shipinstruct = 'DELIVER IN PERSON' "
                             "      ) "
                             "      or "
                             "      ( "
                             "          cast(l_quantity as integer) >= 16 and cast(l_quantity as integer) <= 16 + 10 "
                             "          and l_shipmode in ('AIR', 'AIR REG') "
                             "          and l_shipinstruct = 'DELIVER IN PERSON' "
                             "      ) "
                             "      or "
                             "      ( "
                             "          cast(l_quantity as integer) >= 24 and cast(l_quantity as integer) <= 24 + 10 "
                             "          and l_shipmode in ('AIR', 'AIR REG') "
                             "          and l_shipinstruct = 'DELIVER IN PERSON' "
                             "      ) "
                             "  ) ",
                             'l_partkey',
                             'lineitem_bloom_use',
                             False))

    lineitem_part_join = query_plan.add_operator(join_op())
    filter_op = query_plan.add_operator(filter_def())
    aggregate = query_plan.add_operator(aggregate_def())
    aggregate_project = query_plan.add_operator(aggregate_project_def())
    collate = query_plan.add_operator(collate_op())

    # Connect the operators
    part_scan.connect(part_project)
    part_project.connect(part_bloom_create)
    part_bloom_create.connect(lineitem_bloom_use)
    lineitem_bloom_use.connect(lineitem_project)

    lineitem_part_join.connect_left_producer(lineitem_project)
    lineitem_part_join.connect_right_producer(part_project)

    lineitem_part_join.connect(filter_op)
    filter_op.connect(aggregate)
    aggregate.connect(aggregate_project)
    aggregate_project.connect(collate)

    # Write the plan graph
    query_plan.write_graph(gen_test_id())

    # Start the query
    part_scan.start()
    # lineitem_scan.start()

    # Assert the results
    num_rows = 0
    for t in collate.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    field_names = ['revenue']

    assert len(collate.tuples()) == 1 + 1

    assert collate.tuples()[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    assert collate.tuples()[1] == [92403.0667]

    # Write the metrics
    query_plan.print_metrics()