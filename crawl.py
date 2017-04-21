import boto3
import botocore
import json
import datetime
import tldextract
import hashlib
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
table = dynamodb.Table("crawl-logs")
crawl_seeds_table = dynamodb.Table("crawl-seeds")

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


robots = RobotsCache()
user_agent='OpenHouseProject.co crawler'
sleep_time=.9
bucket = 'oh-crawl'

expiration_rules = {
    'default': datetime.datetime.now() + datetime.timedelta(days=1),
    'starts_with': {
        'http://www.everyhome.com/Home-For-Sale/': datetime.datetime(2099, 1, 1)
      , 'http://www.everyhome.com/Homes-For-Sale-By-Listing-Date/Listed-on-': datetime.datetime(2099, 1, 1)
    }
}

def get_expiration(url, expiration_rules):
    exp = expiration_rules['default']
    sw = expiration_rules['starts_with']
    prefixes = sw.keys()
    for prefix in prefixes:
        if url.startswith(prefix):
            exp = sw[prefix]
    return exp

def crawl_one(url, expiration_rules, headers):
    try:
        allowed = robots.allowed(url, user_agent)
    except:
        allowed = True
    if allowed:
        logger.info('Crawling: ' + url)
        success = False
        content = ''
        r = None
        try:
            r = requests.get(url, headers=headers, timeout=3)
            time.sleep(sleep_time)
            if r.status_code==200:
                content = r.content
            success = True
            sm = decimal.Decimal(2)
        except:
            logger.error('Sleeping due to Connection error')
            time.sleep(10*1)
            try:
                sm = decimal.Decimal(r.status_code)
            except:
                sm = decimal.Decimal(3)
            success = False
        try:
            crawl_seeds_table.update_item(
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
        except:
            # TODO: Better error handing for 400, 500, etc.
            logger.error("Could not update record")
        exp = get_expiration(url, expiration_rules)
        if r == None:
            sc = 404
        else:
            sc = r.status_code
        obj = {'allowed': allowed, 'expiration': exp, 'content': content, 'url': url, 'cache_date': datetime.datetime.now(), 'http_response': sc}
    else:
        # TODO: better error handling
        obj = {"allowed": allowed}
    return obj

def process_one(url, s3, expiration_rules, headers):
    tld = tldextract.extract(url)
    if tld.subdomain != '' and tld.subdomain != 'www':
        tld = tld.subdomain + '.' + tld.domain + '.' + tld.suffix
    else:
        tld = tld.domain + '.' + tld.suffix
    i = url.find(tld)
    s3key = tld + url[i+len(tld):]
    exp = get_expiration(url, expiration_rules)
    try:
        o = s3.ObjectSummary(bucket, s3key)
        lm = o.last_modified
        now = datetime.datetime.utcnow()
        diff = exp - now
        expires_on = now - diff
        if lm.replace(tzinfo=None) < expires_on:
            exists = False
        else:
            exists = True
    except botocore.exceptions.ClientError as e:
        exists = False
    if not(exists):
        logger.info('Processing: ' + url)
        crawl = crawl_one(url, expiration_rules, headers)
        contents = json.dumps(crawl, default=json_util.default)
        fake_handle = StringIO(contents)
        b = s3.create_bucket(Bucket=bucket)
        res = b.put_object(Key=s3key, Body=fake_handle)
        # TODO: check for errors
        dt = datetime.datetime.today().strftime('%Y-%m-%d')
        trackStats(tld, dt, True)
        summaryKey = dt
        trackStats(summaryKey, dt, True)
        summaryKey = tld + "|" + dt
        trackStats(summaryKey, dt, True)
        return True
    return False

def trackStats(summaryKey, date, success):
    if success:
        ue = "set successes = successes + :val"
    else:
        ue = "set errors = errors + :val"
    try:
        return table.update_item(
            Key={
                'summary-key': summaryKey,
                'date': date
            },
            UpdateExpression=ue,
            ExpressionAttributeValues={
                ':val': decimal.Decimal(1)
            },
            ReturnValues="UPDATED_NEW"
        )
    except botocore.exceptions.ClientError:
        s = 0
        e = 0
        if success:
            s = 1
        else:
            e = 1
        return table.put_item(
            Item={
                'summary-key': summaryKey,
                'date': date,
                'successes': s,
                'errors': e
                }
        )

def get_headers(url):
    headers = {}
    if url.startswith('https://www.portlandmaps.com'):
        headers = {
            'Pragma': 'no-cache',
            'Accept-Encoding': 'gzip, deflate, sdch, br',
            'Accept-Language': 'en-US,en;q=0.8',
            'User-Agent': 'OpenHouse crawler',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': '*/*',
            'Cache-Control': 'no-cache',
            'X-Requested-With': 'XMLHttpRequest',
            'Connection': 'keep-alive',
            'Referer': 'https://www.portlandmaps.com/detail/property/',
        }
    return headers

def process_queue(urls, s3, expiration_rules):
    start = datetime.datetime.utcnow()
    count = 0
    cache = {}
    ourls = list(urls)
    while len(urls) > 0:
        url = urls.pop()
        done = cache.has_key(url)
        if not(done):
            headers = get_headers(url)
            try:
                did_work = process_one(url, s3, expiration_rules, headers)
            except botocore.exceptions.ClientError:
                logger.error("error with " + url)
                did_work = 0
            if did_work:
                count += 1
            cache[url] = True
    end = datetime.datetime.utcnow()
    logger.info('Found and processed ' + str(count) + ' unique pages in ' + str(end - start))
    return {"urls": ourls, "count": count, "duration": end - start}

if __name__ == "__main__":
    logger.info("Start")
    did_work = True
    results = []
    while did_work:
        msgs = queue.receive_messages()
        logger.info("Got messages of length " + str(len(msgs)))
        did_work = False
        for msg in msgs:
            did_work = True
            s = msg.body
            o = json.loads(s)
            urls = o
            logger.info(urls)
            resp = process_queue(urls, s3, expiration_rules)
            results.append(resp)
            res = msg.delete()
    #
    sub = 'Crawl results'
    for i in range(len(results)):
        results[i]['duration'] = str(results[i]['duration'])
    if len(results) > 0:
        response = ses.send_email(
            Source='kyle@dataskeptic.com',
            Destination={'ToAddresses': ['kylepolich@gmail.com']},
            Message={
                'Subject': {
                    'Data': sub
                },
                'Body': {
                    'Text': {
                        'Data': json.dumps(results, default=json_util.default)
                    }
                }
            },
            ReplyToAddresses=['kyle@dataskeptic.com']
        )







