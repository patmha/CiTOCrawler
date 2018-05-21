# -*- coding: utf-8 -*-
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import argparse
import json
import sys
import traceback
import os


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
parser.add_argument("-D", "--dfs", action="store_true", help="force use of DFS (Depth-First Search) instead of visit order specified in configuration file")
parser.add_argument("-V", "--visitonly", action="store_true", help="just visit webpages, without RDF parsing")
parser.add_argument("--debugmode", action="store_true", help="debug mode")

# group2 = parser.add_mutually_exclusive_group(required=True)
# group2.add_argument("-B", "--bfs", action="store_true", help="use breadth-first search")
# group2.add_argument("-D", "--dfs", action="store_true", help="use depth-first search")

args = parser.parse_args()#['-sseeds.txt', '--bfs'])
print "In {0}. Args passed in:\n".format(os.path.basename(__file__)), vars(args)

# default configuration file is called "conf.json" and is located in the same directory containing this module.
conf_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'conf.json'))
if args.conf:
    # TODO: accept a relative (to the directory containing this module) path as well
    conf_path = args.conf


# Load configuration from conf_path
conf_dict = {}
try:
    with open(conf_path, "r") as fd:
        file_content = fd.read()
    conf_dict = json.loads(file_content)
except Exception:
    print "\n"
    traceback.print_exc(file=sys.stdout)
    sys.exit(1)
print "\nIn {0}. conf_dict as loaded from JSON configuration file:\n".format(os.path.basename(__file__)), conf_dict

# for k,v in conf_dict.items():
#     print k, v


# Update conf_dict with command line values
if args.seedpath:
    # TODO: accept a relative (to the directory containing this module) path as well
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
    # TODO: accept a relative (to the directory containing this module) path as well
    conf_dict['querypath'] = args.querypath
if args.csvpath:
    # TODO: accept a relative (to the directory containing this module) path as well
    conf_dict['csvpath'] = args.csvpath
if args.depth:
    conf_dict['depth'] = args.depth
if args.dfs:  # use DFS. if not set, visit order will be the one specified in the JSON configuration file (conf_path)
    conf_dict['bfs'] = False
# else:  # args.dfs is not set, therefore BFS is implied
#     conf_dict['bfs'] = True
if args.visitonly:
    conf_dict['visitonly'] = True

# if args.bfs:  # use BFS
#     conf_dict['bfs'] = True
# else:  # args.dfs is set
#     conf_dict['bfs'] = False

# nuconfjson = json.dumps(conf_dict)
# with open("nuconf.json", "w") as f:
#     f.write(nuconfjson)

print "\nIn {0}. conf_dict after update with cmd line args:\n".format(os.path.basename(__file__)), conf_dict

settings = get_project_settings()
# print "\n\nIn CiTO_crawl.py ... project settings BEFORE updating with cmdline args are:"
# for s, v in settings.items():
#     print("{0} = {1}".format(s,v))
# s = settings.get("DEFAULT_REQUEST_HEADERS")
# for k, v in s.items():
#     print(k, v)

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
    # default settings, so no need to update them
    # SCHEDULER_DISK_QUEUE = 'scrapy.squeues.PickleLifoDiskQueue'
    # SCHEDULER_MEMORY_QUEUE = 'scrapy.squeues.LifoMemoryQueue'

# print "\n\n\n\nIn CiTO_crawl.py ... project settings AFTER updating with cmdline args are:"
# for s, v in settings.items():
#     print("{0} = {1}".format(s,v))
# s = settings.get("DEFAULT_REQUEST_HEADERS")
# for k, v in s.items():
#     print(k, v)

# settings.set("LOG_FILE", "TEST.LOG")


process = CrawlerProcess(settings)
process.crawl('cito', **conf_dict)
process.start()  # the script will block here until the crawling is finished
print("\n\n... Done.")
