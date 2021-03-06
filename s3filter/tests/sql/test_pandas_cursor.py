# -*- coding: utf-8 -*-
"""Tests for some select edge cases

"""
import StringIO
import cProfile
import pstats
import timeit

import boto3
import pytest
from boto3 import Session
from botocore.config import Config

from s3filter.plan.query_plan import QueryPlan
from s3filter.sql.pandas_cursor import PandasCursor


def test_non_existent_key():
    """Executes a select against a non existent key.

    :return: None
    """

    cur = PandasCursor(boto3.client('s3'))\
        .select('does-not-exist.csv', 'select * from S3Object')

    try:
        with pytest.raises(Exception):
            cur.execute()
    finally:
        cur.close()


def test_empty_results():
    """Executes a select where no results are returned.

    :return: None
    """

    num_rows = 0

    cur = PandasCursor(boto3.client('s3'))\
        .select('region.csv', 'select * from S3Object limit 0')

    try:
        dfs = cur.execute()
        for df in dfs:
            for i, r in df.iterrows():
                num_rows += 1
                # print("{}:{}".format(num_rows, r))
    finally:
        cur.close()

    assert num_rows == 0


def test_non_empty_results():
    """Executes a select where results are returned.

    :return: None
    """

    num_rows = 0

    cur = PandasCursor(boto3.client('s3'))\
        .select('region.csv', 'select * from S3Object')

    try:
        dfs = cur.execute()
        for df in dfs:
            for i, r in df.iterrows():
                num_rows += 1
                # print("{}:{}".format(num_rows, r))


        assert num_rows == 5
    finally:
        cur.close()

def test_no_ssl():
    """Executes a select where results are returned.

    :return: None
    """

    num_rows = 0

    # Boto is not thread safe so need one of these per scan op
    cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10, s3={'payload_signing_enabled': False})
    session = Session()
    s3 = session.client('s3', use_ssl=False, verify=False, config=cfg)

    cur = PandasCursor(s3)\
        .select('lineitem.csv', 'select * from S3Object limit 150000')

    try:
        pr = cProfile.Profile()
        pr.enable()

        dfs = cur.execute()
        for df in dfs:
            num_rows += len(df)

        pr.disable()
        s = StringIO.StringIO()
        sortby = 'cumulative'
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        print (s.getvalue())

        assert num_rows == 150000
    finally:
        cur.close()

    num_rows = 0

    # Boto is not thread safe so need one of these per scan op
    cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
    session = Session()
    s3 = session.client('s3', config=cfg)

    cur = PandasCursor(s3) \
        .select('lineitem.csv', 'select * from S3Object limit 150000')

    try:
        pr = cProfile.Profile()
        pr.enable()

        dfs = cur.execute()
        for df in dfs:
            num_rows += len(df)

        pr.disable()
        s = StringIO.StringIO()
        sortby = 'cumulative'
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        print (s.getvalue())

        assert num_rows == 150000
    finally:
        cur.close()

def test_where_predicate():
    """Executes a select with a where clause on one of the attributes.

    :return: None
    """

    num_rows = 0

    cur = PandasCursor(boto3.client('s3'))\
        .select('region.csv', 'select * from S3Object where r_name = \'AMERICA\';')

    try:
        dfs = cur.execute()
        for df in dfs:
            for i, r in df.iterrows():
                num_rows += 1
                assert r._1 == 'AMERICA'
                # print("{}:{}".format(num_rows, r))

        assert num_rows == 1
    finally:
        cur.close()


def test_aggregate():
    """Executes a select with an aggregate.

    :return: None
    """

    num_rows = 0

    cur = PandasCursor(boto3.client('s3')) \
        .select('region.csv', 'select count(*) from S3Object')

    try:
        dfs = cur.execute()
        for df in dfs:
            for i, r in df.iterrows():
                num_rows += 1
                assert r._0 == '5'
                # print("{}:{}".format(num_rows, r))

        assert num_rows == 1
    finally:
        cur.close()


def test_large_results():
    """Executes a select where a large number of records are expected

    :return: None
    """

    num_rows = 0

    cur = PandasCursor(boto3.client('s3')) \
        .select('lineitem.csv', 'select * from S3Object limit 150000')

    try:

        # pr = cProfile.Profile()
        # pr.enable()

        start = timeit.default_timer()




        dfs = cur.execute()
        for df in dfs:
            for i, r in df.iterrows():
                num_rows += 1
                # print("{}:{}".format(num_rows, r))
        end = timeit.default_timer()

        elapsed = end - start
        print('Elapsed {}'.format(elapsed))


        # pr.disable()
        # s = StringIO.StringIO()
        # sortby = 'cumulative'
        # ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        # ps.print_stats()
        # print (s.getvalue())

        assert num_rows == 150000
    finally:
        cur.close()


