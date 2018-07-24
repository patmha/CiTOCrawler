# CiTOCrawler

A configurable crawler that looks for embedded RDF (specifically RDF statements using the [CiTO](http://www.sparontologies.net/ontologies/cito) ontology) and stores the resulting graphs with provenance information according to the [OpenCitations Data Model](https://dx.doi.org/10.6084/m9.figshare.3443876).

Developed in **Python 2.7** with Scrapy and RDFLib. The code for storing the OpenCitations-compliant graphs was taken from https://github.com/essepuntato/opencitations and slightly adapted by me.


## Installation
I suggest using a virtual environment for trying out CiTOCrawler. See https://virtualenv.pypa.io/en/stable/ for more information.

The required libraries are listed in `requirements.txt`. To install them simply run the following command:

`$ pip install -r path/to/requirements.txt`


## Usage

To start CiTOCrawler run

`python CiTO_crawl.py <arguments>`

Available arguments can be shown using the `-h` flag:

```
  --conf CONF           absolute path of configuration file
  -s SEEDPATH, --seedpath SEEDPATH
                        absolute path of file containing seed URLs
  -u URLLIST [URLLIST ...], --urllist URLLIST [URLLIST ...]
                        list of seed URLs
  -d DEPTH, --depth DEPTH
                        depth of search; unlimited, if set to 0
  -a ALLOWEDLINKS [ALLOWEDLINKS ...], --allowedlinks ALLOWEDLINKS [ALLOWEDLINKS ...]
                        list of regular expressions for allowed links
  -p PRIORITYLINKS [PRIORITYLINKS ...], --prioritylinks PRIORITYLINKS [PRIORITYLINKS ...]
                        (not implemented yet) list of regular expressions for
                        priority links
  -S, --samedomain      search pages in same domain only
  -m MAXHOURS, --maxhours MAXHOURS
                        max #hours for crawled pages before they get
                        invalidated
  -q QUERYPATH, --querypath QUERYPATH
                        absolute path of file containing SPARQL query
  -c CSVPATH, --csvpath CSVPATH
                        absolute path of CSV with crawled pages and datetime
                        of crawling
  -B, --bfs             force use of DBS (Breadth-First Search) instead of
                        visit order specified in configuration file
  -D, --dfs             force use of DFS (Depth-First Search) instead of visit
                        order specified in configuration file
  -V, --visitonly       just visit webpages, without RDF parsing; (for testing
                        purposes)
  --debug               use debug mode (which generates extra output)
```

Command-line arguments (if passed in) have precedence over the parameters set in the JSON configuration file (eg. the `conf.json` file located in the CiTOCrawler folder).

