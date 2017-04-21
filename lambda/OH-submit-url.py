import json
import boto3
import random

dynamodb = boto3.resource('dynamodb')
client = boto3.client('ses')
table = dynamodb.Table('crawl-seeds')

def lambda_handler(event, context):
    submitter = event['email']
    if submitter == "":
        submitter = "anonymous web entry"
    page = {"uri": event['url']
            , "freq_days": 30
            , "request_followup": event['checked']
            , "submitter": submitter
            , "status-mask": 0
    }
    test = table.get_item(Key={"uri": event['url']})
    found = False
    if test.has_key('Item'):
        found = True
        print("Found")
    if not(found):
        r1 = table.put_item(Item=page)
        check = r1['ResponseMetadata']['HTTPStatusCode']
        if check != 200:
            return {"status": "fail", "msg": "Unable to save order, please contact kyle@dataskeptic.com", "response": r1}
    sub = 'OH URL Submitted'
    if found == True:
        sub += " (DUPLICATE)"
    response = client.send_email(
        Source='kyle@dataskeptic.com',
        Destination={'ToAddresses': ['kylepolich@gmail.com']},
        Message={
            'Subject': {
                'Data': sub
            },
            'Body': {
                'Text': {
                    'Data': json.dumps(event)
                }
            }
        },
        ReplyToAddresses=['kyle@dataskeptic.com']
    )
    if found:
        return {"msg": "We already have that URL, but thank you anyway!!!", "response": response, "status": "ok"}
    else:
        return {"msg": "Got it!  Thanks!!!", "response": response, "status": "ok"}
