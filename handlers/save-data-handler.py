import boto3
import json
from datetime import datetime

from amazon.ion.simpleion import dumps, loads
from pyqldb.driver.qldb_driver import QldbDriver


def insert_document(transaction_executor, document):
        transaction_executor.execute_statement(
            'INSERT INTO user_data ?',
            document
        )


def handler(event, context):
    """
    Creates document for ledger's table from data obtained from AWS SQS message.
    """
    qldb_driver = QldbDriver(ledger_name='my-db', region_name='us-west-2')

    data = json.loads(event['Records'][0]['body'])
    data['datetime'] = datetime.now()

    document_to_insert = loads(dumps(data))

    with qldb_driver as driver:
        driver.execute_lambda(lambda x: insert_document(x, document_to_insert))
        return {
            'statusCode': 200,
            'body': 'Lambda triggered!'
        }
