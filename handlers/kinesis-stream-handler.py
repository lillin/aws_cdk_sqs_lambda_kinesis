import boto3
import base64

from aws_kinesis_agg.deaggregator import deaggregate_records


sqs = boto3.client('sqs')

BENEFIA_QUEUE = 'queue-benefia-company'
ERGOHESTIA_QUEUE = 'queue-ergohestia-company'


def handler(event, context):
    """
    Invokes AWS queue depending on comapany's name.
    """
    raw_kinesis_records = event['Records']

    #Deaggregate all records in one call
    user_records = deaggregate_records(raw_kinesis_records)

    #Iterate through deaggregated records
    payload = []
    for record in user_records:        

        # Kinesis data in Python Lambdas is base64 encoded
        payload.append(base64.b64decode(record['kinesis']['data']))

    print(payload)

    return 'Successfully processed {} records.'.format(len(user_records))
