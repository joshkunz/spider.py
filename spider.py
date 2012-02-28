"""
A asyncronus IO, multiprocessed web spider framework
written in python, using gevent/requests for the async network 
stuff

see /examples/ for example scrapers written with the framework.

Todo:
	Custom error handlers, not just logging
	Fix NoneType Error # not my fault, it's due to 599 responses

"""

from tornado.ioloop import IOLoop
from tornado.httpclient import AsyncHTTPClient, HTTPRequest

from lxml import html
from lxml.html import soupparser
from lxml.etree import XMLSyntaxError

import logging
import functools
import os
import sys

from multiprocessing import Process, cpu_count, get_logger, Queue
from multiprocessing.managers import SyncManager
from Queue import Empty
from threading import Semaphore

LOG_FMT = '%(process)d - %(funcName)s@%(lineno)s %(levelname)s: %(message)s'

class ManagerNotSetError(Exception):
	pass

class Fetcher(Process):
	"""The base class of the scraper"""
	
	def __init__(self, fetchQ, extractors, max_connections=5):
		# Supress the warnings from tornado
		logging.getLogger().setLevel(logging.CRITICAL)
		self.fetch = fetchQ
		self.__extractors = extractors
		self.log = get_logger()

		self.lock = Semaphore(max_connections)

		Process.__init__(self)
        
	def _fetch_url(self, url):
		request = HTTPRequest(url, validate_cert=False)
		self.client.fetch(request, self.process_page)

	def _fill_client(self):
		"""Refill the client's asycronus fetch queue"""
		while self.lock.acquire(False):
			url = self.fetch.get()
			self._fetch_url(url)

	def get(self, url):
		"""Add this URL to the fetching Queue"""
		self.fetch.put(url)
            
	def process_page(self, response):
		"""Parse a page and run the extractors. Used as the callback for 
		http requests."""

		self.lock.release()
		try:
			parsed = html.parse(response.buffer,
								base_url=response.request.url)
		except TypeError:
			parsed = None
		except XMLSyntaxError:
		 	parsed = soupparser.parse(response.buffer)
		
		try:
			for extractor in self.__extractors:
				extractor(self, parsed, response)
		except Exception, e:
			import traceback
			self.log.error(str(traceback.format_exc(limit=10)))
			self.log.error(str(e))

		# Fill the request Queue
		self._fill_client()
        				
	def run(self):
		"""Start the process"""
		# This stays commented for now, for some reason it loses file
		# descriptors, so we'll just use the default AsyncClient
		#AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")
		self.loop = IOLoop()
		self.client = AsyncHTTPClient()
		self.client.initialize(io_loop=self.loop)	
		self._fetch_url(self.fetch.get())
		self.loop.start()

class Scour(object):

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
		
		self._extractors = []
		self._seeds = seed_urls
		
		self.process_count = process_count
		self.islocal = None

		self.log = get_logger()
		if log:
			self.log.setLevel(logging.DEBUG)
			h = logging.FileHandler(log, mode='w')

			h.setLevel(logging.DEBUG)
			h.setFormatter(logging.Formatter(log_format))

			self.log.addHandler(h)

	def _queue_seeds(self):
		for url in self._seeds:
			self.fetch_queue.put(url)
	
	def extractor(self, func):
		"""Decorate a funtions to be run as an extractor"""

		self._extractors.append(func)
		return func

	def run_server(self, address='', port=50000, key=''):
		"""Create a manager for the Scour instance"""
		self.fetch_queue = Queue.Queue()
		self.manager = SyncManager(address=(address, port), authkey=key)	
		self.manager.register('get_fetch_queue',
							  callable=lambda: self.fetch_queue)
		self._queue_seeds()
		self.manager.get_server().serve_forever()

	def local_manager(self):
		"""Make a sync manager to controll the local queue."""
		self.islocal = True
		self.manager = SyncManager()
		self.manager.start()
		self.fetch_queue = self.manager.Queue()
		self._queue_seeds()

	def setup_manager(self, address='', port=50000, key=''):
		self.islocal = False
		self.fetch_queue = Queue()
		self._queue_seeds()

		self.manager = SyncManager(address=(address, port), authkey=key)
		self.manager.register('get_fetch_queue',
							  callable=lambda: self.fetch_queue)

	def setup_processes(self, daemonize=True):
		"""Start the child (fetcher) processes."""

		if not hasattr(self, 'manager'):
			raise ManagerNotSetError("No manager instance")

		self.processes = []
		for process in xrange(self.process_count):
			p = Fetcher(self.fetch_queue, self._extractors)
			p.daemon = daemonize
			p.start()
			print "* Process {process.pid} started".format(process=p)
			self.processes.append(p)

	def run(self, **kwargs):
		"""Start a cluster server master, if 'run' has been called on another
		system its recomended to use the connect_to method instead.
			
		address: ip address of the server
		port: Port to serve on
		key: Authentication key
		"""

		madeManager = not hasattr(self, 'manager')
		print "made", madeManager, not hasattr(self, 'manager'), self.islocal
		if madeManager:	
			self.setup_manager()

		self.setup_processes()
		if self.islocal:
			self.manager.join()
		elif madeManager:
			self.manager = 
		else:
			self.setup_processes()
			print """Using a cluster server...Exiting..."""
		
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
