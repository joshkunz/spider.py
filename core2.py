from tornado import ioloop
from tornado.curl_httpclient import CurlAsyncHTTPClient, HTTPRequest, HTTPResponse
import logging
from BeautifulSoup import BeautifulSoup as bSoup
import functools

class Scour(object):
    "The base class of the scraper"
    
    def __init__(self, seed_urls=[], url_fetch_timeout=25):
        
        ioloop.IOLoop.instance() #instanceiate IOLoop
        self.client = CurlAsyncHTTPClient()
        
        logging.basicConfig(level=logging.DEBUG, filename="m_scrape.log", filemode='w')
        self.log = logging.getLogger(name="mod_scrape")
        
        self.__extractors = []
        
        #Queue Argumet urls
        for url in seed_urls:
            self.get(url)
            
    def get(self, url):
        self.client.fetch(str(url), self.process_page)
            
    def process_page(self, response):
        print "processing page"
        try:
            soup = bSoup(response.body)
            for x in self.__extractors:             #Run the extractors
                x(self, soup, response)
        except Exception, e:                           #Log any errors
            import traceback
            self.log.error(str(traceback.format_exc(limit=10)))
            self.log.error(str(e))
        
    def run(self):
        "run the main loop"
        #start the ioLoop
        ioloop.IOLoop.instance().start()
    
    def extractor(self, func):
        @functools.wraps(func)
        def wrapper(p, soup, response):
            return func(p, soup, response)
            
        self.__extractors.append(wrapper)
        
        return wrapper
    