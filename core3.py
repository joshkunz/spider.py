"""
A asyncronus IO, multiprocessed web spider framework
written in python, using gevent/requests for the async network 
stuff

see basic.py for an example scraper written with the framework.

Todo:
	Custom error handlers, not just logging
	Fix NoneType Error
	Creativly handle HTML parsing errors
	Supress the tornado IOloop Errors

"""

from tornado.ioloop import IOLoop
from tornado.httpclient import AsyncHTTPClient
from lxml import html

import logging
import functools
import os
import sys

from multiprocessing import Process, cpu_count, get_logger
from multiprocessing.managers import SyncManager, Queue
from Queue import Empty
from threading import Semaphore


LOG_FMT = '%(process)d - %(funcName)s@%(lineno)s %(levelname)s: %(message)s'

class ManagerNotSetError(Exception):
	pass

class Fetcher(Process):
	"""The base class of the scraper"""
	
	def __init__(self, fetchQ, extractors, max_connections=5):

		self.fetch = fetchQ
		self.__extractors = extractors
		self.log = get_logger()

		self.lock = Semaphore(max_connections)

		Process.__init__(self)
        
	def _fetch_url(self, url):
		self.client.fetch(url, self.process_page)

	def _fill_client(self):
		while self.lock.acquire(False):
			self._fetch_url(self.fetch.get())

	def get(self, url):
		"Add this URL to the fetching Queue"
		self.fetch.put(url)
            
	def process_page(self, response):
		self.lock.release()
		try:
			parsed = html.parse(response.buffer,
								base_url=response.request.url)
			for extractor in self.__extractors:
				extractor(self, parsed, response)
		except Exception, e:
			import traceback
			self.log.error(str(traceback.format_exc(limit=10)))
			self.log.error(str(e))
		#Check to see if there are no more urls
		self._fill_client()
        				
	def run(self):
		"""Start the process"""
		AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")
		self.loop = IOLoop()
		self.client = AsyncHTTPClient() # Now a CurlAsyncHTTPClient
		self.client.initialize(io_loop=self.loop)
		self._fetch_url(self.fetch.get())
		self.loop.start()

class Scour(object):
	"""Controls the spider processes"""

	def __init__(self, seed_urls=[], process_count=cpu_count(), 
				 log='scrape.log', log_format=LOG_FMT):
		"""Create a new scour instance. The scour class controlls
		the queue, and spawns fetcher processes. Functions to be called when
		a page is finished loading can be added with the 'extractor' decorator
		like so:

		@scour_instance.extractor
		def some_func(process, page, response):
			process.log("Page completed!")

		Params:
			seed_urls: a list of urls to seed the spider
			process_count: number of fetchers to spawn, default to the number
			of cpu's (or cores) found on the system.
			log: The name of the log file. If set to non it will disable 
			logging and any calls to process.log will simple bee noop's.
			default filename is scrape.log
			log_format: the format of each call to process.log, the format is
			the one used in the python logging module documentation for the 
			FileHandler log handler. The default is core3.LOG_FMT
		"""
		
		self.__extractors = []
		self.__seeds = seed_urls
		
		self.process_count = process_count

		self.log = get_logger()
		if log:
			self.log.setLevel(logging.DEBUG)
			h = logging.FileHandler(log, mode="a")
			h.setLevel(logging.DEBUG)
			h.setFormatter(logging.Formatter(log_format))
			self.log.addHandler(h)

	def __queue_seeds(self):
		for url in self.__seeds:
			self.fetch_queue.put(url)
	
	def extractor(self, func):
		"""Decorate a funtions to be run as an extractor"""

		#@functools.wraps(func)
		#def wrapper(process, page, response):
		#	return func(process, page, response)
		
		self.__extractors.append(func)
	
		return func	

	def setup_manager(self, address='', port=50000, key=''):
		"""Create a manager for the Scour instance"""
		self.fetch_queue = Queue.Queue()
		self.manager = SyncManager(address=(address, port), authkey=key)	
		self.manager.register('get_fetch_queue',
							  callable=lambda: self.fetch_queue)
		self.__queue_seeds()

	def setup_processes(self, daemonize=True):
		"""Start the child (fetcher) processes"""

		if not hasattr(self, 'manager'):
			raise ManagerNotSetError("No manager instance")

		self.processes = []
		for process in xrange(self.process_count):
			p = Fetcher(self.fetch_queue, self.__extractors)
			p.daemon = daemonize
			p.start()
			print "* Process {process.pid} started".format(process=p)
			self.processes.append(p)

	def run(self, address='', port=50000, key=''):
		"""Start a cluster server master, if 'run' has been called on another
		system its recomended to use the connect_to method instead.
			
		address: ip address of the server
		port: Port to serve on
		key: Authentication key
		"""

		madeManager = False
		if not hasattr(self, 'manager'):	
			self.setup_manager(address=address, port=port, key=key)
			madeManager = True

		lock = open("scrape.lock", 'w')
		if madeManager:
			lock.write(str(os.getpid()))
			lock.close()
			self.setup_processes()
			self.manager.get_server().serve_forever()
		else:
			self.setup_processes(daemonize=False)
			lock.write("\n".join([str(p.pid) for p in self.processes]))
			lock.close()
			print """Using a cluster server... 
Child Pid's stored in scrape.lock Exiting..."""
		
	def connect_to(self, address='', port=50000, key=''):
		"""Connect to a Scour Server
		
		address: IP address of the server '' is local
		port: Port the server is running on
		key: Auth key the server was started with
		"""

		self.manager = SyncManager(address=(address, port), authkey=key)
		self.manager.register("get_fetch_queue")
		self.manager.connect()
		self.fetch_queue = self.manager.get_fetch_queue()		
