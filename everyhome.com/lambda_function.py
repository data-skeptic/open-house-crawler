import boto3
import zipfile
import gzip
import json
import datetime
import pandas as pd
import BeautifulSoup as soup

from api_push import push

print('Loading function')

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

################################################
# User defined elements necessary for the crawl
################################################

# This dictionary tells the Crawler how long it's ok to cache pages based on the url

expiration_rules = {
    'default': datetime.datetime.now() + datetime.timedelta(days=1),
    'starts_with': {
        'http://www.everyhome.com/Home-For-Sale/': datetime.datetime(2099, 1, 1)
      , 'http://www.everyhome.com/Homes-For-Sale-By-Listing-Date/Listed-on-': datetime.datetime(2099, 1, 1)
    }
}

# This function should take the raw string content of a page, process it, and return the
# desired data structure, or None if something goes wrong (like no content on the page)

def parse_detail_page(b):
    prop = {'raw_address': '', 'bedrooms': -1, 'bathrooms': -1, "size_units": 'I', 'building_size': -1, 'price': -1, 'car_spaces': -1, 'listing_type': 'F', 'features': []}
    other_fields = ['Age', 'Association', 'Basement', 'Cooling', 'Fireplaces', 'Garages', 'Heating', 'Pool', 'Sewer', 'Taxes (Year)', 'Water']
    # TODO: use the extended fields, add them to the list of properties
    tables = b.findAll('table', {'class': 'cell'})
    if len(tables) > 0:
        prop['listing_timestamp'] = datetime.datetime.now()
        addr_rows = b.findAll('td', {'class': 'addr'})
        addr = ' '.join(map(lambda x: x.getText(), addr_rows))
        t = tables[0]
        df = pd.read_html(str(t))[0]
        data = dict(zip(df[0], df[1]))
        prop['raw_address'] = addr
        prop['bedrooms'] = int(data['Bedrooms'])
        prop['bathrooms'] = float(data['Full Baths'] + '.' + data['Partial Baths'])
        if data.has_key('Interior Sq Ft'):
            prop['building_size'] = int(data['Interior Sq Ft'])
        prop['price'] = float(data['Asking Price'].replace('$', '').replace(',', ''))
        if data.has_key('Parking'):
            try:
                prop['car_spaces'] = float(data['Parking'].replace('Cars', '').replace('Car', '').replace(' ', ''))
            except ValueError:
                prop['car_spaces'] = -1
        return [prop]
    else:
        return None

# Takes a string of the raw version of the page and extracts any links we might want to crawl

def process_content(b):
    """Extract further links to crawl from the raw content of a page
    """
    resp = {'content_fail': True, 'links': []}
    resp['content_fail'] = False
    # Look for other pages with links to links to listings
    select = b.find('select')
    if select != None:
        options = select.findAll('option')
        if len(options) > 0:
            others = map(lambda x: x.get('value'), options)
            resp['links'].extend(others)
    # Look for other pages with links to listings
    days = b.findAll('a', {'class': 'days_whch'})
    resp['links'].extend(map(lambda day: day.get('href'), days))
    # Look for properties pages
    properties = b.findAll('td', {'class': 'addr_pcct'})
    for prop in properties:
        link = prop.find('a').get('href')
        resp['links'].append(link)
    return resp

def myconverter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()

def handler(event, context):
    results = []
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
        api_result = push(conf['api_user'], conf['api_passwd'], conf['api_baseurl'], props) # ../utils/api_push.py
        urls = process_content(b)['links']
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName='OH-crawler-url-queue')
        queue.send_message(MessageBody=json.dumps(urls))
        # TODO: On fail, send alert and save to S3
        results.append(result)
    return {"msg": "thank you", "success": True}

if __name__ == "__main__":
    event = json.load(open('test.json', 'r'))
    context = None
    handler(event, context)
