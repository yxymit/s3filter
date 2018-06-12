# -*- coding: utf-8 -*-
"""Group by query tests

"""
import os

from s3filter import ROOT_DIR
from s3filter.op.aggregate import Aggregate
from s3filter.op.aggregate_expression import AggregateExpression
from s3filter.op.collate import Collate
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.op.tuple import LabelledTuple
from s3filter.plan.query_plan import QueryPlan
from s3filter.util.test_util import gen_test_id


def test_aggregate_count():
    """Tests a group by query with a count aggregate

    :return: None
    """

    num_rows = 0

    query_plan = QueryPlan("Count Aggregate Test")

    # Query plan
    # select count(*) from supplier.csv
    ts = query_plan.add_operator(SQLTableScan('supplier.csv', 'select * from S3Object;', 'ts', False))

    a = query_plan.add_operator(Aggregate(
        [
            AggregateExpression(AggregateExpression.COUNT, lambda t_: t_['_0'])
            # count(s_suppkey)
        ],
        'a',
        False))

    c = query_plan.add_operator(Collate('c', False))

    ts.connect(a)
    a.connect(c)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    ts.start()

    # Assert the results
    for t in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    field_names = ['_0']

    assert c.tuples()[0] == field_names
    assert LabelledTuple(c.tuples()[1], field_names)['_0'] == 10000
    assert num_rows == 1 + 1

    # Write the metrics
    query_plan.print_metrics()


def test_aggregate_sum():
    """Tests a group by query with a sum aggregate

    :return: None
    """

    num_rows = 0

    query_plan = QueryPlan("Sum Aggregate Test")

    # Query plan
    # select sum(float(s_acctbal)) from supplier.csv
    ts = query_plan.add_operator(SQLTableScan('supplier.csv', 'select * from S3Object;', 'ts', False))

    a = query_plan.add_operator(Aggregate(
        [
            AggregateExpression(AggregateExpression.SUM, lambda t_: float(t_['_5']))
        ],
        'a',
        False))

    c = query_plan.add_operator(Collate('c', False))

    ts.connect(a)
    a.connect(c)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    ts.start()

    # Assert the results
    for t in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    field_names = ['_0']

    assert c.tuples()[0] == field_names
    assert round(LabelledTuple(c.tuples()[1], field_names)['_0'], 2) == 45103548.65
    assert num_rows == 1 + 1

    # Write the metrics
    query_plan.print_metrics()


def test_aggregate_empty():
    """Executes an aggregate query with no results returned. We tst this as it's somewhat peculiar with s3 select,
    in so much as s3 does not return column names when selecting data, though being an aggregate query we can generate
    the tuple field names based on the expressions supplied.

    TODO: Unsure whether the aggregate operator should return field names. It makes sense in one way, but is different
    to how all the other operators behave.

    :return: None
    """

    num_rows = 0

    query_plan = QueryPlan("Empty Aggregate Test")

    # Query plan
    # select sum(float(s_acctbal)) from supplier.csv limit 0
    ts = query_plan.add_operator(SQLTableScan('supplier.csv', 'select * from S3Object limit 0;', 'ts', False))

    a = query_plan.add_operator(Aggregate(
        [
            AggregateExpression(AggregateExpression.SUM, lambda t_: float(t_['_5']))
        ],
        'a',
        False))

    c = query_plan.add_operator(Collate('c', False))

    ts.connect(a)
    a.connect(c)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    ts.start()

    # Assert the results
    for t in c.tuples():
        num_rows += 1
        print("{}:{}".format(num_rows, t))

    field_names = ['_0']

    assert c.tuples()[0] == field_names
    assert num_rows == 0 + 1

    # Write the metrics
    query_plan.print_metrics()