# -*- coding: utf-8 -*-
"""

"""
from s3filter.plan.op_metrics import OpMetrics
from s3filter.op.operator_base import Operator
from s3filter.op.message import TupleMessage


class Limit(Operator):
    """Represents a limit operator which consumes and produces tuples until it has reached a set maximum number of tuples.

    """
    def __init__(self, max_tuples, name, query_plan, log_enabled):

        super(Limit, self).__init__(name, OpMetrics(), query_plan, log_enabled)

        self.max_tuples = max_tuples
        self.current = 0
        self.first_tuple = True

    def on_receive(self, ms, _producer):
        """Handles the receipt of a message from a producer.

        :param ms: The received messages
        :param _producer: The producer that emitted the message
        :return: None
        """
        for m in ms:
            # print("Top | {}".format(t))

            if type(m) is TupleMessage:
                self.on_receive_tuple(m.tuple_)
            else:
                raise Exception("Unrecognized message {}".format(m))

    def on_receive_tuple(self, tuple_):
        """Handles the receipt of a tuple. When the number of tuples reaches max it informs the producer to stop
        producing. This allows table scans to stop once tuples limit has been reached. It also informs any consumers
        that it is done producing tuples.

        :param tuple_: The received tuple
        :return: None
        """

        if not self.first_tuple:
            self.current += 1
        else:
            self.first_tuple = False

        if self.current <= self.max_tuples:
            self.send(TupleMessage(tuple_), self.consumers)
        elif self.current == self.max_tuples:
            # Set this operator to complete
            if not self.is_completed():
                self.complete()
        else:
            pass


