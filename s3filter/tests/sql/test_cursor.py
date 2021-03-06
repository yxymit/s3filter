# -*- coding: utf-8 -*-
"""Tests for some select edge cases

"""
import timeit

import boto3
import pytest

from s3filter.op.tuple import IndexedTuple
from s3filter.sql.cursor import Cursor


def test_non_existent_key():
    """Executes a select against a non existent key.

    :return: None
    """

    cur = Cursor(boto3.client('s3'))\
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

    cur = Cursor(boto3.client('s3'))\
        .select('region.csv', 'select * from S3Object limit 0')

    try:
        rows = cur.execute()
        for _ in rows:
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

    cur = Cursor(boto3.client('s3'))\
        .select('region.csv', 'select * from S3Object')

    try:
        rows = cur.execute()
        # print("{}:{}".format(num_rows, r))
        for _ in rows:
            num_rows += 1
            # print("{}:{}".format(num_rows, r))

        assert num_rows == 5
    finally:
        cur.close()


def test_where_predicate():
    """Executes a select with a where clause on one of the attributes.

    :return: None
    """

    num_rows = 0

    cur = Cursor(boto3.client('s3'))\
        .select('region.csv', 'select * from S3Object where r_name = \'AMERICA\';')

    try:
        rows = cur.execute()
        for r in rows:
            num_rows += 1
            lt = IndexedTuple.build_default(r)
            assert lt['_1'] == 'AMERICA'
            # print("{}:{}".format(num_rows, r))

        assert num_rows == 1
    finally:
        cur.close()


def test_aggregate():
    """Executes a select with an aggregate.

    :return: None
    """

    num_rows = 0

    cur = Cursor(boto3.client('s3')) \
        .select('region.csv', 'select count(*) from S3Object')

    try:
        rows = cur.execute()
        for r in rows:
            num_rows += 1
            lt = IndexedTuple.build_default(r)
            assert lt['_0'] == '5'
            # print("{}:{}".format(num_rows, r))

        assert num_rows == 1
    finally:
        cur.close()


def test_large_results():
    """Executes a select where a large number of records are expected

    :return: None
    """

    num_rows = 0

    cur = Cursor(boto3.client('s3')) \
        .select('lineitem.csv', 'select * from S3Object limit 150000')

    try:
        start = timeit.default_timer()
        rows = cur.execute()
        for _ in rows:
            num_rows += 1
            # print("{}:{}".format(num_rows, r))

        end = timeit.default_timer()

        elapsed = end - start
        print('Elapsed {}'.format(elapsed))
        assert num_rows == 150000
    finally:
        cur.close()
