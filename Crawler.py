import sys
from reppy.cache import RobotsCache
import boto
import boto.s3.connection
import requests
import time
import datetime

class Crawler(object):
    
    def __init__(self, dao, expiration_rules, extract_content, find_pages_to_crawl, user_agent='OpenHouseProject.co crawler', sleep_time=.9):
        self.robots = RobotsCache()
        self.dao = dao
        self.expiration_rules = expiration_rules
        self.extract_content = extract_content
        self.find_pages_to_crawl = find_pages_to_crawl
        self.user_agent = user_agent
        self.sleep_time = sleep_time
        self.parse_errors = {}
        self.work_done = {}
    
    def crawl_one(self, url, verbose=False):
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
                if verbose:
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
                        if verbose:
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
        

    def process_one_url(self, url, verbose=False):
        """Get page content and parse
           Handles both content pages (with property details) and directory pages (with links to properties)
           Add other links to queue
           Return parse details
        """
        summary = {'link_count': 0, 'cleared_from_cache': False, 'crawled': False, 'parsed': False, 'parse_error': False}
        crawl = self.crawl_one(url, verbose)
        summary['crawled'] = not(crawl['details']['used_cache'])
        content = crawl['content']
        if content == None or content == '':
            summary['parse_error'] = True
            return summary
        summary['parsed'] = self.dao.has_parse(url)
        if crawl['details']['cleared_from_cache']:
            summary['cleared_from_cache'] = True
            if has_parsed:
                self.dao.delete_parse(url)
                summary['parsed'] = False
        parse = None
        saveit = False
        try:
            if not(summary['parsed']):
                if verbose:
                    print 'Parsing', url
                parse = self.extract_content(content['content'])
                saveit = True
                summary['parsed'] = True
        except:
            summary['parse_error'] = True
            saveit = False
        if saveit:
            if verbose:
                print 'saving parse'
            self.dao.save_parsed(url, parse)
        resp = self.find_pages_to_crawl(content['content'])
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
            haskey = self.work_done.has_key(url)
            if not(haskey):
                process = True
            elif haskey:
                hasparsed = self.work_done[url].has_key('parsed')
                if not(hasparsed):
                    process = True
                elif not(self.work_done[url]['parsed']) and not(self.work_done[url]['parse_error']):
                    process = True
            if process:
                count += 1
                if verbose:
                    print('Processing: ' + url)
                result = self.process_one_url(url, verbose)
                for link in result['links']:
                    if not(self.work_done.has_key(link)):
                        queue.append(link)
                    else:
                        wd = self.work_done[link]
                        if wd.has_key('parsed') and not(wd['parsed']):
                            queue.append(link)
                        elif not(wd.has_key('parsed')):
                            queue.append(link)
                self.work_done[url] = result['summary']
        end = datetime.datetime.utcnow()
        if verbose:
            print('Found and processed ' + str(count) + ' unique pages in ' + str(end - start))
        return self.work_done