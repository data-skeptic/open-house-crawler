import requests
import BeautifulSoup as soup
import pandas as pd
import time
import datetime
import pickle
import requests
import ConfigParser

import Crawler
import Dao

###########################
# Load configuration stuff
###########################

propertiesFile = "openhouse.config"
cp = ConfigParser.ConfigParser()
cp.readfp(open(propertiesFile))

access_key = cp.get('AWS', 'access_key')
secret_key = cp.get('AWS', 'secret_key')
bucket_name = cp.get('AWS', 'bucket_name')

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

def parse_detail_page(content):
    prop = {'raw_address': '', 'bedrooms': -1, 'bathrooms': -1, "size_units": 'I', 'building_size': -1, 'price': -1, 'car_spaces': -1, 'listing_type': 'F', 'features': []}
    other_fields = ['Age', 'Association', 'Basement', 'Cooling', 'Fireplaces', 'Garages', 'Heating', 'Pool', 'Sewer', 'Taxes (Year)', 'Water']
    # TODO: use the extended fields
    b = soup.BeautifulSoup(content)
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
        #for of in other_fields:
        #    if data.has_key(of):
        #        prop['features'].append({of: data[of]})
        return [prop]
    else:
        return None

# Takes a string of the raw version of the page and extracts any links we might want to crawl

def process_content(content):
    """Extract further links to crawl from the raw content of a page
    """
    resp = {'content_fail': True, 'links': []}
    if content != '':
        resp['content_fail'] = False
        b = soup.BeautifulSoup(content)
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

################################################
# Instantiate Crawler and execute
################################################

dao = Dao.Dao(access_key, secret_key, bucket_name)
crawler = Crawler.Crawler(dao, expiration_rules, parse_detail_page, process_content)

rootpage = 'http://www.everyhome.com/Homes-For-Sale-By-Listing-Date/Mercer-County-New-Jersey'
crawler.process_queue([rootpage])

