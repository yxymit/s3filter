from s3filter.op.collate import Collate
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.plan.query_plan import QueryPlan
from s3filter.query.tpch import get_file_key
from s3filter.sql.format import Format


def main():
    parts = 32
    query_plan = QueryPlan(is_async=True, buffer_size=0)

    # Query plan
    lineitem_scan = map(lambda p:
                        query_plan.add_operator(
                            SQLTableScan(get_file_key('lineitem', True, p),
                                         "select * from S3Object;", Format.CSV,
                                         use_pandas=True, secure=False, use_native=False,
                                         name='scan_' + str(p), query_plan=query_plan,
                                         log_enabled=False)),
                        range(0, parts))

    collate = query_plan.add_operator(
        Collate('collate', query_plan, False))

    map(lambda (p, o): o.connect(collate), enumerate(lineitem_scan))

    query_plan.execute()


if __name__ == "__main__":
    main()
