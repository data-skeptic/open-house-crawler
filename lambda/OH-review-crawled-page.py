import json
import boto3
import random
import io
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb')
s3 = boto3.resource('s3')
table = dynamodb.Table('crawl-seeds')
bucket = 'oh-crawl'

def tldextract_extract(url):
    url = url.replace("https", "http").replace("http://", "")
    i = url.find('/')
    if i != -1:
        url = url[0:i]
    arr = url.split('.')
    n = len(arr)
    suffix = arr[n-1]
    domain = arr[n-2]
    subdomain = ''
    if n > len(arr):
        subdomain = '.'.join(arr[0:n-2])
    return {"subdomain": subdomain, "domain": domain, "suffix": suffix}

def lambda_handler(event, context):
    # TODO: can we randomize the result so simultaneous workers don't collide?
    # Maybe if it gets that frequent then we should be using SQS instead anyway.
    retry = 10
    response = table.query(
        IndexName='status-mask-index',
        KeyConditionExpression=Key('status-mask').eq(2), Limit=retry)
    #
    if response['Count'] == 0:
        s3key = 'empty.htm'
    else:
        ii = 0
        while ii < retry and ii < response['Count']:
            item = response['Items'][ii]
            url = item['uri']
            tld = tldextract_extract(url)
            if tld['subdomain'] != '' and tld['subdomain'] != 'www':
                tld = tld['subdomain'] + '.' + tld['domain'] + '.' + tld['suffix']
            else:
                tld = tld['domain'] + '.' + tld['suffix']
            i = url.find(tld)
            s3key = tld + url[i+len(tld):]
            print(s3key)
            try:
                obj = s3.Object(bucket, s3key)
                data = io.BytesIO()
                obj.download_fileobj(data)
                s = data.getvalue().decode("utf-8")
                resp = {"content": s, "url": url}
                return resp
            except:
                pass
            ii += 1
    s3key = 'empty.htm'
    obj = s3.Object(bucket, s3key)
    data = io.BytesIO()
    obj.download_fileobj(data)
    s = data.getvalue().decode("utf-8")
    resp = {"content": s, "url": "done"}
    return resp
