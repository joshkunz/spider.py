from tornado import ioloop
from tornado.curl_httpclient import CurlAsyncHTTPClient, HTTPRequest, HTTPResponse
from multiprocessing import Process, Manager, Queue, Semaphore, Lock, cpu_count
from BeautifulSoup import BeautifulSoup as bSoup
import urlparse
import robotparser as rParser
import cPickle as cP
import threading
import functools
import logging
import sys
import copy

#### Internal Utilities ####

class ResponseClone(object):
    pass

class RobotUtils(object):
    
    @classmethod
    def check_robots(cls, url, user_agent="*", cache_redis=False,
                     redis_server="localhost", redis_db="dupe"):
        "Check to see if a page allows robots on that url"
        
        if cache_redis: rRobots = redis.Redis(redis_server, db=redis_db)
        
        linkParse = urlparse.urlparse(url)
        id = (linkParse.scheme if linkParse.scheme != "" else "http") + "://" + linkParse.netloc
        
        if cache_redis and rRobots.exists(id) != True:
            robots = cls.add_to_redis(rRobots, redis_server, redis_db)
        elif cache_redis and rRobots.exsits(id):
            robots = cP.loads(rRobots.get(id))
        elif not cache_redis:
            robots = rParser.RobotFileParser()
            robots.set_url(id + "/robots.txt")
            robots.read()
            
        return robots.can_fetch(user_agent, url)
        
    def add_to_redis(r_conn, redis_server, redis_db):
        robots = rParser.RobotFileParser()
        robots.set_url(id + "/robots.txt")
        robots.read()
        r_conn.set(id, cP.dumps(robots))
        r_conn.expire(id, 7200)
        return robots

############################

###### Error Classes #######

class RobotsRestricted(Exception):
    
    def __init__(self, url_to_access):
        self.url = url_to_access
        self.message = "Robots are not allowed on this page"
    def __str__(self):
        return self.message
    
class UrlMalformed(Exception):
    
    def __init__(self, url_to_access):
        self.url = url_to_access
        self.message = "Url is malformed"
    def __str__(self):
        return self.message
    
#############################

##### Process Classes ######

class PageProcessorBase(Process):
    "The base class for page_processors"
    
    def __init__(self, parent, log_queue, page_queue, http_client, extractors=[], robot_info={}, __robot_util=RobotUtils):
        "Initialize the Process"
        self.__log = log_queue
        self.__pqueue = page_queue
        self.__client = http_client
        self.__parent = parent
        self.__extractors = extractors
        self.__robot_info = robot_info
        self.__robot = __robot_util
        Process.__init__(self)
        
    def run(self):
        while True:
            page_item = self.__pqueue.get() #Blocking get waiting to parse input
            
            try:
                soup = bSoup(page_item.body)
                for x in self.__extractors:             #Run the extractors
                    x(self, soup, page_item)
            except Exception, e:                           #Log any errors
                import traceback
                self.log(str(traceback.format_exc(limit=10)), level="error")
                self.log(str(e), level="error")
            
    def log(self, message, level="debug"):
        "A logging helper function to clean up the code"
        self.__log.put({"level": level, "entry": str(message)})
    
    def queue_url(self, url):
        "add a url to the feed"
        if self.__robot_info["obey_robots"] and not self.__robot.check_robots(url, **self.__robot_info["redis"]):
            return None
        elif self.__robot_info["obey_robots"]:
            return None
        print "Queueing"
        self.__client.fetch(HTTPRequest(str(url)), self.__parent.put_page)


class Scour(object):
    "The base class of the scraper"
    
    def __init__(self, process_count=None, seed_urls=[], seed_file=None,
                 process_class=PageProcessorBase, url_fetch_timeout=25, obey_robots=True,
                 redis={}):
        
        self.__manager = Manager()
        ioloop.IOLoop.instance() #instanceiate IOLoop
        self.http_client = CurlAsyncHTTPClient()
        
        self.page_queue = self.__manager.Queue()
        self.log_queue = self.__manager.Queue()
        
        logging.basicConfig(level=logging.DEBUG, filename="m_scrape.log", filemode='w')
        self.log = logging.getLogger(name="mod_scrape")
        
        self.process_handler = process_class
        self.process_count = process_count
        
        self.timeout = url_fetch_timeout
        self.robot_info = {
            "obey_robots": obey_robots,
            "redis": redis
        }
        
        if self.process_count == None:
            try:
                self.log.log(logging.INFO, "No process argument supplied, attempting to use cpu_count")
                self.process_count = cpu_count()
                self.log.log(logging.INFO, "Process count set to: %s"% self.process_count)
            except NotImplementedError:
                self.log.log(logging.ERROR, "Method cpu_count failed, defaulting to 1")
                self.process_count = 1
        
        self.__extractors = []
        
        #Queue File Urls
        if seed_file:
            read_thread = threading.Thread(target=self.read_thread, args=(seed_file,))
            read_thread.daemon =True
            read_thread.start()
        
        #Queue Argumet urls
        for url in seed_urls:
            self.__queue_url(url)
            
    def __queue_url(self, url): #only a helper function please use the process's method
        if self.robot_info["obey_robots"] and not RobotUtils.check_robots(url, **self.robot_info["redis"]):
            return None
        elif self.robot_info["obey_robots"]:
            return None
        self.http_client.fetch(url, self.put)
        
    def read_thread(self, file):
        for url in file:
            self.__queue_url(url)
        
    @staticmethod
    def log_thread(log_queue, log):
        level_map = {
            "debug": logging.DEBUG,
            "warning": logging.WARN,
            "critical": logging.CRITICAL,
            "error": logging.ERROR,
            "info": logging.INFO
        }
        
        while True:
            message = log_queue.get()
            log.log(level_map[message["level"]], message["entry"])
            
    def put_page(self, response):
        print "PAGE RECIVED: {0} {1}".format(response.code, response.request.url)
        response.headers = dict(response.headers.get_all())
        response.request.headers = dict(response.request.headers.get_all())
        resp = ResponseClone()
        resp.body = response.body
        response.__dict__.pop("buffer")
        resp.__dict__.update(copy.deepcopy(response.__dict__))
            
        self.page_queue.put(resp)
            
    def run(self):
        "run the main loop"
        
        #Start the logging thread
        self.logger = threading.Thread(target=self.log_thread, args=(self.log_queue, self.log,))
        self.logger.setDaemon(True)
        self.logger.start()
        
        #Spawn the job processors
        for p_num in xrange(self.process_count):
            iprocess = self.process_handler(self,
                                            self.log_queue,
                                            self.page_queue,
                                            self.http_client,
                                            extractors=self.__extractors,
                                            robot_info=self.robot_info)
            iprocess.daemon = True
            iprocess.start()
        
        #start the ioLoop
        ioloop.IOLoop.instance().start()
    
    def extractor(self, func):
        @functools.wraps(func)
        def wrapper(p, soup, response):
            return func(p, soup, response)
            
        self.__extractors.append(wrapper)
        
        return wrapper
