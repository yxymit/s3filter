# -*- coding: utf-8 -*-
"""Bloom tests

"""
import boto3

from s3filter.hash.bloom_filter import BloomFilter
from s3filter.hash.scalable_bloom_filter import ScalableBloomFilter
from s3filter.hash.sliced_bloom_filter import SlicedBloomFilter
from s3filter.hash.sliced_sql_bloom_filter import SlicedSQLBloomFilter
from s3filter.sql.cursor import Cursor


def test_bloom_filter():
    num_keys = 10000
    capacity = num_keys
    fp_rate = 0.1

    bf = SlicedBloomFilter(capacity, fp_rate)

    # Fill up the bloom filter to capacity
    for i in range(0, num_keys):
        bf.add(i)

    # Test for keys we know should be there and keys we know are missing
    num_positive = 0
    num_false_positive = 0
    for i in range(0, num_keys):

        known_positive_k = i
        known_negative_k = num_keys + i + 1

        if known_positive_k in bf:
            num_positive += 1

        if known_negative_k in bf:
            num_false_positive += 1

    observed_positive_rate = float(num_positive) / float(num_keys)
    observed_false_positive_rate = float(num_false_positive) / float(num_keys)

    # Diagnostics
    # print("Positive rate {}".format(observed_positive_rate))
    # print("False positive rate {}".format(observed_false_positive_rate))

    # Assert all the added keys are present
    assert observed_positive_rate == 1.0

    # Assert that we have a false positive rate within acceptable bounds (i.e. < 1 decimal place)
    assert observed_false_positive_rate < fp_rate * 10.0


def test_scalable_bloom():
    num_keys = 10000
    initial_capacity = 10
    fp_rate = 0.01

    bf = ScalableBloomFilter(initial_capacity, fp_rate, ScalableBloomFilter.LARGE_SET_GROWTH)

    # Fill up the bloom filter to capacity
    for i in range(0, num_keys):
        bf.add(i)

    # Test for keys we know should be there and keys we know are missing
    num_positive = 0
    num_false_positive = 0
    for i in range(0, num_keys):

        known_positive_k = i
        known_negative_k = num_keys + i + 1

        if known_positive_k in bf:
            num_positive += 1

        if known_negative_k in bf:
            num_false_positive += 1

    observed_positive_rate = float(num_positive) / float(num_keys)
    observed_false_positive_rate = float(num_false_positive) / float(num_keys)

    # Diagnostics
    # print("Positive rate {}".format(observed_positive_rate))
    # print("False positive rate {}".format(observed_false_positive_rate))

    # Assert all the added keys are present
    assert observed_positive_rate == 1.0

    # Assert that we have a false positive rate within acceptable bounds. This is tricky as the filter is probabalistic
    # We tst for a little over 1 decimal place + 100%
    assert observed_false_positive_rate < fp_rate * 10.0 * 2.0


def test_sliced_bloom_sql():
    # Values to add to our our bloom filter
    v1 = '12'
    v2 = '64'

    bf = SlicedSQLBloomFilter(SlicedBloomFilter(100, 0.3))
    bf.add(int(v1))
    bf.add(int(v2))

    cur = Cursor(boto3.client('s3')).select('part.csv',
                                            "select "
                                            "   p_partkey, "
                                            "   {},"
                                            "   {} "
                                            "from "
                                            "   S3Object "
                                            "where "
                                            "   {}"
                                            .format(
                                                bf.build_hash_functions_sql_projection('p_partkey'),
                                                bf.build_bit_array_strings_sql_projection(),
                                                bf.build_bit_array_string_sql_predicate('p_partkey')))

    print("{}".format(cur.s3sql))

    try:

        rows = cur.execute()

        v1_present = False
        v2_present = False

        num_rows = 0
        for r in rows:
            num_rows += 1
            if r[0] == v1:
                v1_present = True
            if r[0] == v2:
                v2_present = True

            print("{}:{}".format(num_rows, r))

        assert v1_present
        assert v2_present
        assert num_rows < 200000

    finally:
        cur.close()


def test_bloom_size_check():
    best_p = SlicedSQLBloomFilter.calc_best_fp_rate(200000)
    bf = SlicedSQLBloomFilter(SlicedBloomFilter(200000, best_p))

    pass


def test_bloom_scan():
    bf = None

    cur = Cursor(boto3.client('s3')).select('part.csv',
                                            "select "
                                            "   p_partkey "
                                            "from "
                                            "   S3Object limit 1000")

    try:

        rows = cur.execute()
        rows_list = list(rows)
        best_p = SlicedSQLBloomFilter.calc_best_fp_rate(len(rows_list))
        bf = SlicedSQLBloomFilter(SlicedBloomFilter(len(rows_list), 0.01))

        for r in rows_list:
            bf.add(int(r[0]))

    finally:
        cur.close()

    sql = "select    l_partkey from   S3Object where {}" \
        .format(bf.build_bit_array_string_sql_predicate('l_partkey'))

    cur = Cursor(boto3.client('s3')).select('lineitem.csv',
                                            sql)

    try:

        rows = cur.execute()
        for r in rows:
            print(int(r[0]))

    finally:
        cur.close()

# TODO: Reimplement these

# def test_random_bloom_sql():
#     sample_size = 100
#     capacity = sample_size
#     fp_rate = 0.95
#     num_parts = 200000
#
#     bf = BloomFilter(capacity, fp_rate)
#
#     # Values to add to our our bloom filter, selected fron all 200000 part keys
#     key_list = random.sample(range(num_parts), capacity)
#     for k in key_list:
#         bf.add(k)
#
#     cur = Cursor().select('part.csv',
#                           "select "
#                           "   p_partkey "
#                           "from "
#                           "   S3Object "
#                           "where "
#                           "   {}"
#                           .format(bf.sql_predicate('p_partkey')))
#
#     # print("{}".format(cur.s3sql))
#
#     try:
#
#         rows = cur.execute()
#
#         row_list = list(rows)
#
#         for k in key_list:
#             found = False
#             for r in row_list:
#                 if int(r[0]) == k:
#                     found = True
#                     break
#
#             # print("key: {}, found: {}".format(k, found))
#
#             assert found
#
#         assert len(row_list) >= sample_size
#         assert len(row_list) < num_parts
#
#     finally:
#         cur.close()


# def test_random_scalable_bloom_sql():
#     initial_capacity = 10
#     sample_size = 100
#     fp_rate = 0.01
#     num_parts = 200000
#
#     bf = ScalableBloomFilter(initial_capacity, fp_rate, ScalableBloomFilter.LARGE_SET_GROWTH)
#
#     # Values to add to our our bloom filter, selected fron all 200000 part keys
#     key_list = random.sample(range(num_parts), sample_size)
#     for k in key_list:
#         bf.add(k)
#
#     cur = Cursor().select('part.csv',
#                           "select "
#                           "   p_partkey "
#                           "from "
#                           "   S3Object "
#                           "where "
#                           "   {}"
#                           .format(bf.sql_predicate('p_partkey')))
#
#     # print("{}".format(cur.s3sql))
#
#     try:
#
#         rows = cur.execute()
#
#         row_list = list(rows)
#
#         for k in key_list:
#             found = False
#             for r in row_list:
#                 if int(r[0]) == k:
#                     found = True
#                     break
#
#             # print("key: {}, found: {}".format(k, found))
#
#             assert found
#
#         assert len(row_list) >= sample_size
#         assert len(row_list) < num_parts
#
#     finally:
#         cur.close()
