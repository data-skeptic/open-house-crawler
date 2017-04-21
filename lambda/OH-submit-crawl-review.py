import json
import boto3
import random
import io
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb')
s3 = boto3.resource('s3')
table = dynamodb.Table('crawl-seeds')
bucket = 'oh-crawl'

def lambda_handler(event, context):
    print(event)
    url = event['url']
    sm = event['status-mask']
    table.update_item(
        Key={
            'uri': url
        },
        UpdateExpression="set #sm = :val",
        ExpressionAttributeNames={
            '#sm': 'status-mask'
        },
        ExpressionAttributeValues={
            ':val': sm
        },
        ReturnValues="UPDATED_NEW"
    )
    resp = {"status": "done"}
    return resp
