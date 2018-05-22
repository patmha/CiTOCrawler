# -*- coding: utf-8 -*-
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import argparse
import json
import sys
import traceback
import os


# Define command-line arguments
parser = argparse.ArgumentParser(description="Crawls the Web looking for pages with embedded RDF using the CiTO ontology")
parser.add_argument("--conf", type=str, help="absolute path of configuration file")
group1 = parser.add_mutually_exclusive_group(required=False)
group1.add_argument("-s", "--seedpath", type=str, help="absolute path of file containing seed URLs")
group1.add_argument("-u", "--urllist", nargs='+', type=str, help="list of seed URLs")
parser.add_argument("-d", "--depth", type=int, help="depth of search; unlimited, if set to 0")
parser.add_argument("-a", "--allowedlinks", nargs='+', type=str, help="list of regular expressions for allowed links")
parser.add_argument("-p", "--prioritylinks", nargs='+', type=str, help="(not implemented yet) list of regular expressions for priority links")
parser.add_argument("-S", "--samedomain", action="store_true", help="search pages in same domain only")
parser.add_argument("-m", "--maxhours", type=int, help="max #hours for crawled pages before they get invalidated")
parser.add_argument("-q", "--querypath", type=str, help="absolute path of file containing SPARQL query")
parser.add_argument("-c", "--csvpath", type=str, help="absolute path of CSV with crawled pages and datetime of crawling")
group2 = parser.add_mutually_exclusive_group(required=False)
group2.add_argument("-B", "--bfs", action="store_true", help="force use of DBS (Breadth-First Search) instead of visit order specified in configuration file")
group2.add_argument("-D", "--dfs", action="store_true", help="force use of DFS (Depth-First Search) instead of visit order specified in configuration file")
parser.add_argument("-V", "--visitonly", action="store_true", help="just visit webpages, without RDF parsing; (for testing purposes)")
parser.add_argument("--debug", action="store_true", help="use debug mode (which generates extra output)")

# Parse command-line arguments
args = parser.parse_args()#['-sseeds.txt', '--bfs'])
print "In {0}. Args passed in:\n".format(os.path.basename(__file__)), vars(args)

# Get full path of the default configuration file "conf.json" (located in the same directory containing this module)
conf_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'conf.json'))
if args.conf:
    conf_path = args.conf


# Load configuration from conf_path
conf_dict = {}
try:
    with open(conf_path, "r") as fd:
        file_content = fd.read()
    conf_dict = json.loads(file_content)
    # TODO: check keys in JSON configuration file.
except Exception:
    print "\n"
    traceback.print_exc(file=sys.stdout)
    sys.exit(1)
print "\nIn {0}. conf_dict as loaded from JSON configuration file:\n".format(os.path.basename(__file__)), conf_dict

# Update conf_dict with command line values
if args.seedpath:
    conf_dict['seedpath'] = args.seedpath
elif args.urllist:
    conf_dict['urllist'] = args.urllist
if args.allowedlinks:
    conf_dict['allowedlinks'] = args.allowedlinks
if args.prioritylinks:
    conf_dict['prioritylinks'] = args.prioritylinks
if args.samedomain:
    conf_dict['samedomain'] = args.samedomain
if args.maxhours:
    conf_dict['maxhours'] = args.maxhours
if args.querypath:
    conf_dict['querypath'] = args.querypath
if args.csvpath:
    conf_dict['csvpath'] = args.csvpath
if args.depth:
    conf_dict['depth'] = args.depth
if args.bfs:
    conf_dict['bfs'] = True
elif args.dfs:
    conf_dict['bfs'] = False
# if a visit order flag was not set, visit order will be BFS or DFS, depending on the value for key "bfs" in conf_dict

if args.visitonly:
    conf_dict['visitonly'] = True
if args.debug:
    conf_dict['debug'] = True

print "\nIn {0}. conf_dict after update with cmd line args:\n".format(os.path.basename(__file__)), conf_dict


# get Scrapy default project settings (i.e. those in settings.py)
settings = get_project_settings()  # returns a Settings object

# update default project settings with conf_dict values
if conf_dict.get('depth') is not None:
    settings.set('DEPTH_LIMIT', conf_dict['depth'])
else:
    print "Depth of search was not specified in configuration file nor as a cmd line argument!"
    sys.exit(1)
if conf_dict['bfs'] is True:
    settings.set('DEPTH_PRIORITY', 100)
    settings.set('SCHEDULER_DISK_QUEUE', 'scrapy.squeues.PickleFifoDiskQueue')
    settings.set('SCHEDULER_MEMORY_QUEUE', 'scrapy.squeues.FifoMemoryQueue')
else:  # conf.bfs is False, which means DFS should be used
    settings.set('DEPTH_PRIORITY', -100)
    # these are the default settings, so no need to update them
    # SCHEDULER_DISK_QUEUE = 'scrapy.squeues.PickleLifoDiskQueue'
    # SCHEDULER_MEMORY_QUEUE = 'scrapy.squeues.LifoMemoryQueue'


# Create CrawlerProcess object with scrapy.settings.Settings instance passed in
process = CrawlerProcess(settings)
# Run crawler by creating Crawler object for spider class associated with spider name and by calling its crawl method
# which then instantiates a CitoSpider object with **conf_dict as arguments,
# "while setting the execution engine in motion.
# Returns a deferred that is fired when the crawl is finished." (quoted from Scrapy documentation)
process.crawl('cito', **conf_dict)
# Start Twisted reactor
process.start()  # the script will block here until the crawling is finished
print("\n\n... Done.")
