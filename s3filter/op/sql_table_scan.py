# -*- coding: utf-8 -*-
"""

"""
import cProfile
import time

from s3filter.op.message import TupleMessage
from s3filter.op.operator_base import Operator
from s3filter.op.tuple import Tuple, IndexedTuple
from s3filter.plan.op_metrics import OpMetrics
from s3filter.sql.cursor import Cursor
from s3filter.util.constants import *
# noinspection PyCompatibility,PyPep8Naming
import cPickle as pickle

from s3filter.sql.pandas_cursor import PandasCursor


class SQLTableScanMetrics(OpMetrics):
    """Extra metrics for a sql table scan

    """

    # These amounts vary by region, but let's assume it's a flat rate for simplicity
    COST_S3_DATA_RETURNED_PER_GB = 0.0007
    COST_S3_DATA_SCANNED_PER_GB = 0.002

    def __init__(self):
        super(SQLTableScanMetrics, self).__init__()

        self.rows_returned = 0

        self.time_to_first_response = 0
        self.time_to_first_record_response = None
        self.time_to_last_record_response = None

        self.query_bytes = 0
        self.bytes_scanned = 0
        self.bytes_processed = 0
        self.bytes_returned = 0

    def cost(self):
        """
        Estimates the cost of the scan operation based on S3 pricing in the following page:
        <https://aws.amazon.com/s3/pricing/>
        :return: The estimated cost of the table scan operation
        """
        return self.bytes_returned * BYTE_TO_GB * SQLTableScanMetrics.COST_S3_DATA_RETURNED_PER_GB + \
                self.bytes_scanned * BYTE_TO_GB * SQLTableScanMetrics.COST_S3_DATA_SCANNED_PER_GB

    def __repr__(self):
        return {
            'elapsed_time': round(self.elapsed_time(), 5),
            'rows_returned': self.rows_returned,
            'query_bytes': self.query_bytes,
            'bytes_scanned': self.bytes_scanned,
            'bytes_processed': self.bytes_processed,
            'bytes_returned': self.bytes_returned,
            'time_to_first_response': round(self.time_to_first_response, 5),
            'time_to_first_record_response':
                None if self.time_to_first_record_response is None
                else round(self.time_to_first_record_response, 5),
            'time_to_last_record_response':
                None if self.time_to_last_record_response is None
                else round(self.time_to_last_record_response, 5),
            'cost': self.cost()

        }.__repr__()


class SQLTableScan(Operator):
    """Represents a table scan operator which reads from an s3 table and emits tuples to consuming operators
    as they are received. Generally starting this operator is what begins a query.

    """

    def on_receive(self, message, producer_name):
        raise NotImplementedError

    def __init__(self, s3key, s3sql, use_pandas, name, query_plan, log_enabled):
        """Creates a new Table Scan operator using the given s3 object key and s3 select sql

        :param s3key: The object key to select against
        :param s3sql: The s3 select sql
        """

        super(SQLTableScan, self).__init__(name, SQLTableScanMetrics(), query_plan, log_enabled)

        self.s3 = query_plan.s3

        self.s3key = s3key
        self.s3sql = s3sql

        self.use_pandas = use_pandas

    def run(self):
        """Executes the query and begins emitting tuples.

        :return: None
        """

        if not self.is_profiled:
            self.do_run()
        else:
            cProfile.runctx('self.do_run()', globals(), locals(), self.profile_file_name)

    def do_run(self):

        self.op_metrics.timer_start()

        if self.log_enabled:
            print("{} | {}('{}') | Started"
                  .format(time.time(), self.__class__.__name__, self.name))

        if self.use_pandas:
            cur = self.execute_pandas_query(self)
        else:
            cur = self.execute_py_query(self)

        self.op_metrics.bytes_scanned = cur.bytes_scanned
        self.op_metrics.bytes_processed = cur.bytes_processed
        self.op_metrics.bytes_returned = cur.bytes_returned
        self.op_metrics.time_to_first_record_response = cur.time_to_first_record_response
        self.op_metrics.time_to_last_record_response = cur.time_to_last_record_response

        if not self.is_completed():
            self.complete()

        self.op_metrics.timer_stop()

    @staticmethod
    def execute_py_query(op):
        cur = Cursor(op.s3).select(op.s3key, op.s3sql)
        tuples = cur.execute()
        first_tuple = True
        for t in tuples:

            if op.is_completed():
                break

            op.op_metrics.rows_returned += 1

            if first_tuple:
                # Create and send the record field names
                it = IndexedTuple.build_default(t)
                first_tuple = False

                if op.log_enabled:
                    print("{}('{}') | Sending field names: {}"
                          .format(op.__class__.__name__, op.name, it.field_names()))

                op.send(TupleMessage(Tuple(it.field_names())), op.consumers)

            # if op.log_enabled:
            #     print("{}('{}') | Sending field values: {}".format(op.__class__.__name__, op.name, t))

            op.send(TupleMessage(Tuple(t)), op.consumers)
        return cur

    @staticmethod
    def execute_pandas_query(op):
        cur = PandasCursor(op.s3).select(op.s3key, op.s3sql)
        dfs = cur.execute()
        op.op_metrics.query_bytes = cur.query_bytes
        op.op_metrics.time_to_first_response = op.op_metrics.elapsed_time()
        first_tuple = True
        for df in dfs:

            assert (len(df) > 0)

            if first_tuple:
                assert (len(df.columns.values) > 0)
                op.send(TupleMessage(Tuple(df.columns.values)), op.consumers)
                first_tuple = False

                if op.log_enabled:
                    print("{}('{}') | Sending field names: {}"
                          .format(op.__class__.__name__, op.name, df.columns.values))

            op.op_metrics.rows_returned += len(df)

            op.send(df, op.consumers)
        return cur

    def on_producer_completed(self, producer_name):
        """This event is overridden really just to indicate that it never fires.

        :param producer_name: The completed producer
        :return: None
        """

        raise NotImplementedError
