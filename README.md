spider.py
============

An asynchronous, multiprocessed, python spider framework.

## Getting Started

The spider is seperated into two parts, the actuall engine and the extractors.
The engine submits the requests, and handles all of the processes and 
connections. The extractors are functions that are registered to be called
after a page has been loaded and parsed.

The engine is represented as the Scour object.

	import spider
	scour = spider.Scour(seeds_urls=[])

Extractors are registed using the scour object and the extractor decorator.

	@scour.extractor
	def do_somthing(process, page, response):
		pass

Or they can be registed by passing the function to `scour.extractor`
	
	scour.extractor(lambda process, page, response: True)

After all of the extractors have been registed the actual spider can be run

	scour.run()

Which will start up multiple processes and begin downloading the pages in
its queue. Extractors can pass in urls using `process.get`

There's also alot of documentation in spider.py, and it's not very long
only ~300 lines.

## Very Basic Example

More complete examples can be found in the /examples/ folder. (*see [basic.py]("https://github.com/Joshkunz/spider.py/blob/master/examples/basic.py", "basic.py"))

	import spider
	
	# Don't actually use google, your spider won't get far
	seeds = ["http://google.com"]
	scour = spider.Scour(seed_urls=seeds)

	scour.extractor
	def churn_urls(process, page, response):
		"""Put all of the urls on the page into the Queue.
		
		process: The process this callback is running in.
			process.log.{info,debug,warn, etc..} to write to the log file
			process.get(url) to add a url to the queue

		page: lxml.html representation of the page or None if no page 
			could be parsed

		response: Tornado response object
		"""
		urls = page.xpath("//a/@href") #get a list of the urls on a page
		for url in urls:
			process.get(url)

	scour.run()
