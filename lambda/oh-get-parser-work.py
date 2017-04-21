import boto3
from boto3.dynamodb.conditions import Key, Attr
import random

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('crawl-seeds')

def lambda_handler(event, context):
    work = {}
    response = table.query(KeyConditionExpression=Key('status-mask').eq(1),IndexName='status-mask-index')
    items = response['Items']
    i = int(random.random() * len(items))
    if len(items) > 0:
        item = items[i]
        work['item'] = item
        # Get crawl content 
        work['html'] = ''
    return work
    
    