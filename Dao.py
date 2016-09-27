import tldextract
import uuid
import hashlib
import json
import calendar, datetime, time
import boto
import boto.s3.connection

class Dao(object):
    
    def __init__(self, access_key, secret_key, bucket_name):
        self.conn = boto.connect_s3(
            aws_access_key_id = access_key,
            aws_secret_access_key = secret_key,
            host = 's3.amazonaws.com',
            calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )
        self.bucket = self.conn.get_bucket(bucket_name)
    
    def get_cached_content(self, url):
        """Retrieve page content object, but only if it's in the cache.
           Returned object has keys: content, cache_date, and expiration.
        """
        path = self.get_filename(url)
        key = self.bucket.get_key(path + '/content.json')
        s = key.get_contents_as_string()
        content = json.loads(s)
        # This is a workaround for a bug introduced that has old crawled pages in a different format
        if content.has_key('details'):
            print 'Legacy fix'
            content = content['content']
        # Another fix
        if not(content.has_key('cache_date')):
            print 'Error', url
        return content
    
    def get_parse(self, url):
        path = self.get_filename(url)
        key = self.bucket.get_key(path + '/parsed.json')
        s = key.get_contents_as_string()
        return json.loads(s)
    
    def parse_s3_date(self, s):
        modified = time.strptime(s, '%a, %d %b %Y %H:%M:%S %Z')
        dt = datetime.datetime.fromtimestamp(time.mktime(modified))
        return dt
    
    def get_expiration(self, url):
        now = datetime.datetime.utcnow()
        path = self.get_filename(url)
        key = self.bucket.get_key(path + '/content.json')
        if key is None:
            return datetime.datetime(1970,1,1)
        dt = self.parse_s3_date(key.last_modified)
        if dt < now:
            self.delete_content(url)
            self.delete_parse(url)
            return now
        return dt
    
    def has_parse(self, url):
        path = self.get_filename(url)
        key = self.bucket.get_key(path + '/parsed.json')
        if key is not None:
            return True
        else:
            return False
    
    def delete_parse(self, url):
        path = self.get_filename(url)
        self.bucket.delete_key(path + '/parsed.json')
    
    def delete_content(self, url):
        path = self.get_filename(url)
        self.bucket.delete_key(path + '/content.json')
    
    def default(self, obj):
        """Default JSON serializer."""
        if isinstance(obj, datetime.datetime):
            if obj.utcoffset() is not None:
                obj = obj - obj.utcoffset()
            millis = int(
                calendar.timegm(obj.timetuple()) * 1000 +
                obj.microsecond / 1000
            )
            return millis
        raise TypeError('Not sure how to serialize %s' % (obj,))
    
    def save_content(self, url, obj):
        path = self.get_filename(url)
        content = json.dumps(obj, default=self.default)
        key = self.bucket.new_key(path + '/content.json')
        key.set_contents_from_string(content)

    def save_parsed(self, url, obj):
        path = self.get_filename(url)
        content = json.dumps(obj, default=self.default)
        key = self.bucket.new_key(path + '/parsed.json')
        key.set_contents_from_string(content)

    def extract_bucket_name(self, url):
        tld = tldextract.extract(url)
        bucket_name = tld.subdomain + '.' + tld.domain + '.' + tld.suffix
        return bucket_name

    def get_filename(self, url):
        bucket_name = self.extract_bucket_name(url)
        m = hashlib.md5()
        m.update(url)
        key = m.hexdigest()
        return bucket_name + '/' + key