import spider
import urlparse
from random import randint

seeds = ["http://www.cse.psu.edu/~groenvel/urls.html"]
GOOD_SCHEMES = ('http', 'https')

scan = spider.Scour(seed_urls=seeds)
if not hasattr(scan, 'manager'):
	scan.local_manager()
seen_urls = scan.manager.list()

def iter_unique_urls(url_list):
	global seen_urls
	for link in url_list:
		parsed = urlparse.urlparse(link)
		fixed_link = u"{url.scheme}://{url.netloc}{url.path}".format(url=parsed)
		if parsed.scheme in GOOD_SCHEMES and parsed.netloc \
		and fixed_link not in seen_urls:
			seen_urls.append(fixed_link)
			yield fixed_link

@scan.extractor
def print_response(process, page, response):
	global seen_urls
	process.log.info(
		"SEEN: {0}, RESPONSE: {r.code} {r.request.url}"\
		.format(len(seen_urls), r=response)
	)

@scan.extractor
def add_links(process, page, response):
	if page is None: return
	for link in iter_unique_urls(page.xpath('//a/@href')):
		process.get(link)

print 'Starting...'
scan.run()
