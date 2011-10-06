from tornado import ioloop
from tornado.curl_httpclient import CurlAsyncHTTPClient, HTTPRequest, HTTPResponse
import logging
from BeautifulSoup import BeautifulSoup as bSoup
import functools
from multiprocessing import Process, cpu_count, get_logger
from multiprocessing.managers import SyncManager, Queue

class Fetcher(Process):
    "The base class of the scraper"
    
    def __init__(self, fetchQ, extractors, url_fetch_timeout=25):
        
        self.fetch = fetchQ
        self.__extractors = extractors
        self.log = get_logger()
        Process.__init__(self)
        
    def client_has_urls(self):
        try:
            #Trying to pop an item off the list will throw a IndexError if no items exist
            self.client._requests.append(self.client._requests.pop())
        except IndexError:
            return False
        return True
    
    def __fetch_url(self, url):
        self.client.fetch(str(url), self.process_page)
            
    def get(self, url):
        self.fetch.put(url)
            
    def process_page(self, response):
        try:
            soup = bSoup(response.body)
            for x in self.__extractors:#Run the extractors
                x(self, soup, response)
        except Exception, e:                           #Log any errors
            import traceback
            self.log.error(str(traceback.format_exc(limit=10)))
            self.log.error(str(e))
        if not self.client_has_urls():
            self.log.info("Grabbing Links...")
            map(self.__fetch_url, [self.fetch.get() for z in xrange(6)])
        
    def run(self):
        ##This instanceiates the loop in the child process to ensure that the
        ##file discriptors don't die when the process in started
        self.loop = ioloop.IOLoop.instance() #instanceiate IOLoop
        self.client = CurlAsyncHTTPClient()
        self.client.initialize(io_loop=self.loop)
        
        self.__fetch_url(self.fetch.get())
        self.loop.start()
       
class Scour(object):
    
    def __init__(self, seed_urls=[], process_count=5):
        
        self.__extractors = []
        self.__seeds = seed_urls
        
        self.process_count = process_count
        
        self.log = get_logger()
        self.log.setLevel(logging.DEBUG)
        h = logging.FileHandler("m_scrape.log", mode="a")
        h.setLevel(logging.DEBUG)
        h.setFormatter(logging.Formatter("%(process)d - %(funcName)s@%(lineno)s %(levelname)s: %(message)s"))
        self.log.addHandler(h)
        
    def extractor(self, func):
        @functools.wraps(func)
        def wrapper(p, soup, response):
            return func(p, soup, response)
            
        self.__extractors.append(wrapper)
        
        return wrapper
    
    def p_stats(self):
        while True:
            from time import sleep
            sleep(10)
            print "Stats:"
            print "\n".join("{0}: {1}".format(process.pid, "Running..." if process.is_alive() else "Stopped...") for process in self.processes)
            
    def __queue_seeds(self):
        for url in self.__seeds:
            self.fetch_queue.put(url)
    
    def serve(self):
        "Start a server process instead of running itself..."
        self.fetch_queue = Queue.Queue()
        self.__queue_seeds()
        self.manager = SyncManager(address=('', 50000), authkey="ThisIsATestKey")
        self.manager.register("get_fetch_queue", callable=lambda: self.fetch_queue)
        self.server = self.manager.get_server()
        self.server.serve_forever()
        
    def connect_to(self, key, address='', port=50000):
        self.manager = SyncManager(address=(address, port), authkey=key)
        self.manager.register("get_fetch_queue")
        self.manager.connect()
        self.fetch_queue = self.manager.get_fetch_queue()
        self.run()
    
    def run(self):
        if not hasattr(self, "manager"):
            self.manager = SyncManager().start()
            self.fetch_queue = self.manager.Queue()
            self.__queue_seeds()
        
        self.processes = []
        for process in xrange(self.process_count):
            p = Fetcher(self.fetch_queue, self.__extractors)
            p.daemon =True
            p.start()
            self.processes.append(p)
            
        self.p_stats()
            