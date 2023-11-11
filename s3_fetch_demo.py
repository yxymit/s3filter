from boto3 import Session
from botocore.config import Config



cfg = Config(region_name="us-east-2", parameter_validation=False, max_pool_connections=10)
session = Session()
s3 = session.client('s3', config=cfg)

s3sql = "select * from S3Object where L_ORDERKEY = '1';"

response = s3.select_object_content(
    Bucket="s3filter-289785222077",
    Key="access_method_benchmark/shards-1GB/lineitem.1.csv",
    ExpressionType='SQL',
    Expression=s3sql,
    InputSerialization={
        'CSV': {'FileHeaderInfo': 'Use', 'RecordDelimiter': '\n', 'FieldDelimiter': '|'}},
    OutputSerialization={'CSV': {}}
)

for event in response['Payload']:
    print(event)
    if 'Records' in event:
        # Process records
        records = event['Records']['Payload'].decode('utf-8')
        print(records)