import json
import time
import boto3

def lambda_handler(event, context):
    resp = {'count': 0}
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='OH-crawler-url-queue')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table("crawl-seeds")
    items = table.scan()['Items']
    now = time.time()
    for item in items:
        uri = item['uri']
        if item.has_key('last_crawl'):
            last = float(item['last_crawl'])
        else:
            last = 0
        if now - last > 24*60*60:
            print("Updating: " + uri)
            r1 = queue.send_message(MessageBody=json.dumps([uri]))
            timestamp = int(time.time())
            resp['count'] += 1
            r2 = table.update_item(
                Key={
                    'uri': uri
                },
                UpdateExpression="set last_crawl = :n, #sm = :m",
                ExpressionAttributeNames={
                    '#sm': 'status-mask'
                },
                ExpressionAttributeValues={
                    ':n': timestamp,
                    ':m': 1
                },
                ReturnValues="UPDATED_NEW"
            )
    return resp

