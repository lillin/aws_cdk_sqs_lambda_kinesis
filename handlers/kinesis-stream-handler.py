import boto3
import base64
import json

from amazon.ion.simpleion import dumps, loads
from aws_kinesis_agg.deaggregator import iter_deaggregate_records


sqs = boto3.client('sqs')

BENEFIA_QUEUE = 'https://sqs.us-west-2.amazonaws.com/765423797119/queue-benefia-company'
ERGOHESTIA_QUEUE = 'https://sqs.us-west-2.amazonaws.com/765423797119/queue-ergohestia-company'


def send_to_benefia_queue():
    sqs.send_message(
        QueueUrl=BENEFIA_QUEUE,
        DelaySeconds=10,
        MessageAttributes={},
        MessageBody=('Company is Benefia!')
    )


def send_to_ergohestia_queue():
    sqs.send_message(
        QueueUrl=ERGOHESTIA_QUEUE,
        DelaySeconds=10,
        MessageAttributes={},
        MessageBody=('Company is Ergohestia!')
    )


queue_map = {
    'One': send_to_benefia_queue,
    'Two': send_to_ergohestia_queue,
}


def handler(event, context):
    """
    Invokes AWS queue depending on comapany's name.
    """
    raw_kinesis_records = event['Records']

    payload = None
    for record in iter_deaggregate_records(raw_kinesis_records):        
        # Kinesis data in Python Lambdas is base64 encoded
        b_rec = base64.b64decode(record['kinesis']['data'])

        payload = loads(b_rec, single_value=True)['payload']

    try:
        company = payload['revision']['data']['company']
        queue_map[company]()
        print('Message sent!')
    except KeyError:
        print('Such company does not exist!')
