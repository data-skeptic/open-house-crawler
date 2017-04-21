import boto3
import botocore
import json
import datetime
import tldextract
import hashlib
from boto3.dynamodb.conditions import Key, Attr
import time
import reppy
from reppy.cache import RobotsCache
import requests
from bson import json_util
from cStringIO import StringIO
import decimal
import logging
import sys

logname = sys.argv[0]

logger = logging.getLogger(logname)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
logger.setLevel(logging.INFO)

hdlr = logging.FileHandler('/var/tmp/' + logname + '.log')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 

stdout = logging.StreamHandler()
stdout.setFormatter(formatter)
logger.addHandler(stdout)

s3 = boto3.resource('s3')
sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName='OH-crawler-url-queue')
ses = boto3.client('ses')

dynamodb = boto3.resource('dynamodb')
crawl_seeds_table = dynamodb.Table("crawl-seeds")

response = crawl_seeds_table.query(
    IndexName='status-mask-index',
    KeyConditionExpression=Key('status-mask').eq(1), Limit=20)

items = response['Items']

for item in items:
	uri = item['uri']
	queue.send_message(MessageBody=json.dumps([uri]))

