import boto3
import botocore
import json
import datetime
import tldextract
import hashlib
import time
from reppy.cache import RobotsCache
import requests
from bson import json_util
from cStringIO import StringIO

s3 = boto3.resource('s3')
sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName='OH-crawler-url-queue')

robots = RobotsCache()
user_agent='OpenHouseProject.co crawler'
sleep_time=.9

expiration_rules = {
    'default': datetime.datetime.now() + datetime.timedelta(days=1),
    'starts_with': {
        'http://www.everyhome.com/Home-For-Sale/': datetime.datetime(2099, 1, 1)
      , 'http://www.everyhome.com/Homes-For-Sale-By-Listing-Date/Listed-on-': datetime.datetime(2099, 1, 1)
    }
}

def get_bucket(url):
	tld = tldextract.extract(url)
	base = 'open-house-crawl-'
	if tld.subdomain != '' and tld.subdomain != 'www':
		bucket_name = base + tld.subdomain + '.' + tld.domain + '.' + tld.suffix
	else:
		bucket_name = base + tld.domain + '.' + tld.suffix
	return bucket_name

def get_s3_key(url):
	tld = tldextract.extract(url)
	if tld.subdomain != '' and tld.subdomain != 'www':
		tldname = tld.subdomain + '.' + tld.domain + '.' + tld.suffix
	else:
		tldname = tld.domain + '.' + tld.suffix
	m = hashlib.md5()
	m.update(url)
	key = m.hexdigest()
	return tldname + '/' + key + '/content.json'

def get_expiration(url, expiration_rules):
    exp = expiration_rules['default']
    sw = expiration_rules['starts_with']
    prefixes = sw.keys()
    for prefix in prefixes:
        if url.startswith(prefix):
            exp = sw[prefix]
    return exp

def crawl_one(url, expiration_rules, headers):
    allowed = robots.allowed(url, user_agent)
    if allowed:
        print 'Crawling', url
        success = False
        content = ''
        failure_tollerance=2
        while not(success) and failure_tollerance > 0:
            try:
                r = requests.get(url, headers=headers)
                time.sleep(sleep_time)
                if r.status_code==200:
                    content = r.content
                success = True
            except requests.exceptions.ConnectionError:
                if verbose:
                    print 'Sleeping due to Connection error'
                failure_tollerance -= 1
                time.sleep(60*10)
        # TODO: Better error handing for 400, 500, etc.
        exp = get_expiration(url, expiration_rules)
        obj = {'expiration': exp, 'content': content, 'url': url, 'cache_date': datetime.datetime.now(), 'http_response': r.status_code}
    else:
        # TODO: better error handling
        obj = {}
    return obj

def process_one(url, s3, expiration_rules, headers):
	bucket = get_bucket(url)
	s3key = get_s3_key(url)
	try:
		o = s3.ObjectSummary(bucket, s3key)
		exp = get_expiration(url, expiration_rules)
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
		print('Processing: ' + url)
		crawl = crawl_one(url, expiration_rules, headers)
		contents = json.dumps(crawl, default=json_util.default)
		fake_handle = StringIO(contents)
		res = s3.Bucket(bucket).put_object(Key=s3key, Body=fake_handle)
		# TODO: check for errors
		return True
	return False

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
	while len(urls) > 0:
		url = urls.pop()
		done = cache.has_key(url)
		if not(done):
			headers = get_headers(url)
			did_work = process_one(url, s3, expiration_rules, headers)
			if did_work:
				count += 1
			cache[url] = True
	end = datetime.datetime.utcnow()
	print('Found and processed ' + str(count) + ' unique pages in ' + str(end - start))

did_work = True

while did_work:
	msgs = queue.receive_messages()
	did_work = False
	for msg in msgs:
		did_work = True
		s = msg.body
		o = json.loads(s)
		urls = o
		process_queue(urls, s3, expiration_rules)
		res = msg.delete()
	if not(did_work):
		time.sleep(60)
