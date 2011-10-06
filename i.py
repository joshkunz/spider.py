import core3 as ms
import redis
import urlparse

seeds = [
	"http://www.cse.psu.edu/~groenvel/urls.html"
]

rServer = redis.Redis("localhost", db="dupe")

def validate_url(url):
    "Checks to see if the url includes at least a scheme and a netloc "
    p_url = urlparse.urlparse(url)
    if p_url.scheme in ["http", "https"] and p_url.netloc != "" and not rServer.exists(url):
        return True
    else:
        return False

scan = ms.Scour(seed_urls=seeds, process_count=ms.cpu_count())

@scan.extractor
def print_response(p, soup, resp):
	rServer.incr("got_count")
	p.log.info("RESPONSE: {0} {1} {2}".format(resp.code, resp.request.url, resp.request_time))
	
@scan.extractor
def churn_urls(p, soup, resp):
	all_urls = soup.findAll("a")
	unique_pages = set(x["href"] for x in all_urls if x.get("href", None) != None and "#" not in x["href"] and x["href"] != "")
	for url in unique_pages:
		if validate_url(url):
			rServer.set(url, 1)
			p.get(url)
		
import sys
if sys.argv[1:] and sys.argv[1] == "serve":
    print "Serving"
    scan.serve()
elif sys.argv[1:] and sys.argv[1] == "cluster":
	print "Connecting...."
	scan.connect_to("ThisIsATestKey")
else:
	print "Running normally"
	scan.run()