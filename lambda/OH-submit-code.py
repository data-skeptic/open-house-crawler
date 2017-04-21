import json
import datetime
import boto3

dynamodb = boto3.resource('dynamodb')
s3 = boto3.resource('s3')
client = boto3.client('ses')
table = dynamodb.Table('crawl-seeds')
bucket = 'oh-crawl'

def lambda_handler(event, context):
    if event.has_key('subject'):
        subject = event['subject']
    else:
        subject = 'OpenHouse code submission at ' + str(datetime.datetime.now())
        url = event['url']
    sm = 60
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
    response = client.send_email(
        Source='kyle@dataskeptic.com',
        Destination={'ToAddresses': ['kyle@dataskeptic.com']},
        Message={
            'Subject': {
                'Data': subject
            },
            'Body': {
                'Text': {
                    'Data': json.dumps(event)
                }
            }
        },
        ReplyToAddresses=['kyle@dataskeptic.com']
    )
    return {"msg": "ok", "response": response}