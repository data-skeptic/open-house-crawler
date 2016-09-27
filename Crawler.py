import sys
from reppy.cache import RobotsCache
import boto
import boto.s3.connection
import requests
import time
import datetime

class Crawler(object):
    
    def __init__(self, dao, expiration_rules, parse_func, extract_content, user_agent='OpenHouseProject.co crawler', sleep_time=.9):
        self.robots = RobotsCache()
        self.dao = dao
        self.expiration_rules = expiration_rules
        self.parse_func = parse_func
        self.extract_content = extract_content
        self.user_agent = user_agent
        self.sleep_time = sleep_time
        self.parse_errors = {}
        self.work_done = {}
    
    def crawl_one(self, url):
        """ Leverages cache to prevent duplicate crawls
            Checks for a single page in cache, if available
            Crawls for page if not in cache and not expired
            Returns page content and details about the crawl
        """
        resp = {'cleared_from_cache': False, 'used_cache': True}
        failure_tollerance = 3
        expiration = self.dao.get_expiration(url)
        now = datetime.datetime.utcnow()
        if expiration > now:
            obj = self.dao.get_cached_content(url)
            resp['using_content_from'] = obj['cache_date']
        else:
            resp['using_content_from'] = now
            resp['used_cache'] = False
            allowed = self.robots.allowed(url, self.user_agent)
            if allowed:
                print 'Crawling', url
                success = False
                content = ''
                while not(success) and failure_tollerance > 0:
                    try:
                        r = requests.get(url)
                        time.sleep(self.sleep_time)
                        resp['http_response'] = r.status_code
                        if r.status_code==200:
                            content = r.content
                        success = True
                    except requests.exceptions.ConnectionError:
                        print 'Sleeping due to Connection error'
                        failure_tollerance -= 1
                        time.sleep(60*10)
                # TODO: Better error handing for 400, 500, etc.
                exp = self.expiration_rules['default']
                sw = self.expiration_rules['starts_with']
                prefixes = sw.keys()
                for prefix in prefixes:
                    if url.startswith(prefix):
                        exp = sw[prefix]
                obj = {'expiration': exp, 'content': content, 'cache_date': datetime.datetime.now()}
                self.dao.save_content(url, obj)
            else:
                obj = ''
                # TODO: better error handling
        return {'content': obj, 'details': resp}
    
    def log_error(self, url, err):
        if not(self.parse_errors.has_key(err)):
            self.parse_errors[err] = []
        self.parse_errors[err].append(url)
        

    def process_one_url(self, url):
        """Get page content and parse
           Handles both content pages (with property details) and directory pages (with links to properties)
           Add other links to queue
           Return parse details
        """
        summary = {'has_content': False, 'link_count': 0, 'cleared_from_cache': False, 'crawled': False, 'parse': False, 'parse_error': False}
        crawl = self.crawl_one(url)
        content = crawl['content']
        has_parsed = dao.has_parse(url)
        if crawl['details']['cleared_from_cache']:
            summary['cleared_from_cache'] = True
            if has_parsed:
                self.dao.delete_parse(url)
                has_parsed = False
        parse = None
        try:
            if not(has_parsed):
                print 'Parsing', url
                parse = self.parse_func(content['content'])
                summary['parse'] = True
        except:
            summary['parse_error'] = True
            parse = None
        if parse is not None:
            print 'saving parse'
            self.dao.save_parsed(url, parse)
        resp = self.extract_content(content['content'])
        links = resp['links']
        nlinks = []
        for link in links:
            if link != url:
                dt = self.dao.get_expiration(link)
                queueit = False
                if dt < datetime.datetime.utcnow():
                    queueit = True
                elif not(self.work_done.has_key(link)):
                    queueit = True
                elif not(self.dao.has_parse(link)):
                    queueit = True
                if queueit and self.work_done.has_key(link):
                    wd = self.work_done[link]
                    if wd != {}:
                        queueit = False
                if queueit:
                    nlinks.append(link)
                    self.work_done[link] = {} # Adding this stub helps reduce redundant finds of the same page
        summary['link_count'] = len(nlinks)
        return {'summary': summary, 'links': nlinks}

    def process_queue(self, queue, verbose=False):
        """Process queue elements one by one until complete"""
        start = datetime.datetime.utcnow()
        count = 0
        while len(queue) > 0:
            url = queue.pop()
            process = False
            if not(self.work_done.has_key(url)):
                process = True
            elif not(self.work_done[url].has_key('used_cache')):
                process = True
            if process:
                count += 1
                if verbose:
                    print url
                result = self.process_one_url(url)
                queue.extend(result['links'])
                self.work_done[url] = result['summary']
        end = datetime.datetime.utcnow()
        if verbose:
            print('Found and processed ' + str(count) + ' unique pages in ' + str(end - start))
        return self.work_done