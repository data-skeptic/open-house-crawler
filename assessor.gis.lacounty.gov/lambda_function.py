import boto3
import zipfile
import gzip
import json
import datetime
import pandas as pd
import BeautifulSoup as soup

from api_push import push

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

expiration_rules = {}

def parse_detail_page(b):
    s = b.getText()
    outer = json.loads(s)
    o = json.loads(outer)
    properties = []
    for feature in o['features']:
        prop = feature['attributes']
        p = {'raw_address': prop['SAADDR'] + ' ' + prop['SAADDR2']
         , 'bedrooms': prop['BATHROOMS']
         , 'bathrooms': prop['BEDROOMS']
         , "size_units": 'I'
         , 'building_size': prop['SIZE']
         , 'price': prop['SALEPRICE']
         , 'car_spaces': -1
         , 'listing_type': 'F'
         , 'features': [{'year_built': prop['YEARBUILT']}]
        }
        properties.append(p)
    return properties

def process_content(b):
    resp = {'content_fail': False, 'links': []}

def handler(event, context):
    f = open('api_creds.conf', 'r')
    lines = f.readlines()
    f.close()
    conf = {}
    for line in lines:
        k,v = line.split('=')
        conf[k] = v.strip()
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        if key[0]=='/':
            key = key[1:]
        fname = '/tmp/content.json'
        print([bucket, key])
        s3_client.download_file(bucket, key, fname)
        o = json.load(open(fname, 'r'))
        content = o["content"]
        b = soup.BeautifulSoup(content)
        props = parse_detail_page(b)
        if props != None:
            print(["Num properties", len(props)])
            if len(props) > 0:
                api_result = push(conf['api_user'], conf['api_passwd'], conf['api_baseurl'], props) # ../utils/api_push.py
                # TODO: On fail, send alert and save to S3
        urls = process_content(b)['links']
        if len(urls) > 0:
            sqs = boto3.resource('sqs')
            queue = sqs.get_queue_by_name(QueueName='OH-crawler-url-queue')
            queue.send_message(MessageBody=json.dumps(urls))
    return {"msg": "thank you", "success": True}

if __name__ == "__main__":
    event = json.load(open('test.json', 'r'))
    context = None
    handler(event, context)
