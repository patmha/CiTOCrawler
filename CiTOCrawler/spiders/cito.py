# -*- coding: utf-8 -*-
import scrapy
from scrapy import signals

from CiTOCrawler.graph_util import *
from rdflib import Graph, URIRef
from CiTOCrawler.OC.script.conf_spacin import *
from CiTOCrawler.OC.script.graphlib import *
from CiTOCrawler.OC.script.resource_finder import *
import os
import csv
from datetime import datetime
import sys
import traceback
import logging
from urlparse import urlsplit
import re


class CitoSpider(scrapy.spiders.Spider):
    name = 'cito'

    main_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    crawled_pages_dir = os.path.join(main_dir, "crawled_pages")
    crawled_rdf_dir = os.path.join(main_dir, "crawled_RDF")
    crawled_cito_dir = os.path.join(main_dir, "crawled_CiTO")

    page_counter = 0
    hasRDF = False
    pageWithRDF_counter = 0
    hasCiTOStatements = False
    pageWithCiTOStatements_counter = 0
    globalGraph = Graph()

    resource_counter = 0
    url2oc_dict = {}

    def __init__(self, *args, **kwargs):
        logger = logging.getLogger('chardet.charsetprober')
        logger.setLevel(logging.INFO)
        super(CitoSpider, self).__init__(*args, **kwargs)

        # Use values of 'urrlist' or 'seedpath' key to set start_urls spider attribute
        if kwargs.get('urllist'):
            self.start_urls = kwargs['urllist']
        elif kwargs.get('seedpath'):
            with open(kwargs['seedpath'], "r") as fd:
                self.start_urls = [x.strip('\n') for x in
                                   fd.readlines()]  # taken from https://stackoverflow.com/questions/3277503/how-do-i-read-a-file-line-by-line-into-a-list
        else:
            print 'Seed URLS missing in configuration file!\n' \
                  'Please set a value for either the "seedpath" or "urllist" key in configuration file.\n' \
                  'Note: if value for "urllist" is not empty, it will have precedence over value for "seedpath".'
            os._exit(1)
        print "start_urls:", self.start_urls

        # Use value of 'csvpath' to set crawled_urls_dict spider attribute
        if kwargs.get('csvpath'):
            self.csvpath = kwargs['csvpath']
        else:
            self.csvpath = os.path.join(main_dir, 'crawledPages.csv')  # set a default value
        print "csvpath is {0}".format(self.csvpath)

        self.crawled_urls_dict = {}  # set a default value
        if os.path.isfile(self.csvpath):
            with open(self.csvpath, "r") as f:
                reader = csv.DictReader(f, fieldnames=('url', 'date'))
                try:
                    for row in reader:
                        self.crawled_urls_dict[row['url']] = row['date']
                except csv.Error as e:
                    print "Something went wrong trying to read csv file"
                    sys.exit('file %s, line %d: %s' % (kwargs['csvpath'], reader.line_num, e))  # https://docs.python.org/2/library/csv.html
            print "\nLoaded crawled_urls_dict from {0}".format(self.csvpath)
        else:  # file csvpath may not exist when script is run for the first time
            print self.csvpath, "does not exist. crawled_urls_dict =", self.crawled_urls_dict

        self.maxhours = 720  # set a default value
        if kwargs.get('maxhours'):
            self.maxhours = kwargs['maxhours']  # change default value for maxhours to the one passed in
        print "maxhours is:", self.maxhours

        # Use value of 'querypath' to set sparql_query spider attribute
        self.sparql_query = """CONSTRUCT { ?s ?p ?o . }
                               WHERE {
                                    ?s ?p ?o .
                                    FILTER regex(str(?p), "^http://purl.org/spar/cito/.*")
                                    }"""  # set a default value
        if kwargs.get('querypath'):
            try:
                with open(kwargs['querypath'], "r") as fd:
                    self.sparql_query = fd.read()
            except Exception as e:
                print "\n"
                traceback.print_exc(file=sys.stdout)
                print "\n\n"
                info_str = u"Ooops: {0}: {1}".format(e.__class__.__name__, str(e))
                print info_str, "\n...shutting down."
                os._exit(1)
        print "sparql_query is:", self.sparql_query

        # Use value of 'allowedlinks' to set allowedlinks spider attribute
        self.allowedlinks = []  # set a default value
        if kwargs.get('allowedlinks'):
            self.allowedlinks = kwargs['allowedlinks']
        print "allowedlinks is:", self.allowedlinks

        # Use value of 'prioritylinks' to set prioritylinks spider attribute
        self.prioritylinks = []  # set a default value
        if kwargs.get('prioritylinks'):
            self.prioritylinks = kwargs['prioritylinks']
        print "prioritylinks is:", self.prioritylinks

        # Use value of 'samedomain' to set samedomain spider attribute
        self.samedomain = False  # set a default value
        if kwargs.get('samedomain'):  # i.e., samedomain key is found and value associated to key evaluates to True
            self.samedomain = True
        print "samedomain is:", self.samedomain

        # Use value of 'visitonly' to set visitonly spider attribute
        self.visitonly = False  # set a default value
        if kwargs.get('visitonly'):  # i.e., visitonly key is found and value associated to key evaluates to True
            self.visitonly = True
        print "visitonly is:", self.visitonly

        # Use value of 'debug' to set debug spider attribute
        self.debug = False  # set a default value
        if kwargs.get('debug'):  # i.e., debug key is found and value associated to key evaluates to True
            self.debug = True
        print "debug is:", self.debug

    def start_requests(self):
        for i, url in enumerate(self.start_urls):
            yield scrapy.Request(url=url, callback=self.parse, meta={"url4base": url})

    def parse(self, response):
        # Acceptable content-type is either text/html or application/xhtml+xml
        content_type = response.headers.get('Content-Type')
        if content_type is None or 'html' not in content_type:
            info_str = u'Ignoring response with unacceptable Content-Type: ' + (
                content_type if content_type is not None else "[not set]")
            print 'Ignoring response with unacceptable Content-Type: ' + (
                content_type if content_type is not None else "[not set]")
            self.logger.info(info_str)

        else:
            print 'contentype  =================================================' + content_type

            # Check if page URL (after possible redirection) has been crawled before less than self.maxhours ago
            last_crawl_time = self.crawled_urls_dict.get(response.url)
            if last_crawl_time and self._data_still_valid(last_crawl_time):
                info_str = u"Ignoring URL: crawled {0} recently.".format(response.url)
                self._print_and_log(info_str)
            else:  # page has never been crawled or was crawled more than self.maxhours ago

                self.hasRDF = False
                self.hasCiTOStatements = False

                with open(self.csvpath, 'a') as csvfile:
                    crawl_writer = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
                    crawl_writer.writerow(
                        (response.url, datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')))

                # Save crawled page
                url_pieces = response.url.split('/')
                filename1 = '_'.join(url_pieces)
                self.page_counter += 1
                filename = 'page' + str(self.page_counter) + '_' + filename1  # + '.html'

                try:
                    if not os.path.exists(self.crawled_pages_dir):
                        os.mkdir(self.crawled_pages_dir)
                    with open(os.path.join(self.crawled_pages_dir, filename), 'wb') as f:
                        f.write(response.body)
                except IOError as e:
                    if e.errno == 63:
                        filename = filename[:100] + 'CUTSHORT'
                        with open(os.path.join(self.crawled_pages_dir, filename), 'ab') as f:
                            f.write(response.body)
                            # Add original page URL, for debugging purposes
                            f.write('\n\n"trueurl" : "{0}"'.format(response.url))
                    else:
                        # this shouldn't happen, normally
                        info_str = u"Ooops: {0}: {1}".format(e.__class__.__name__, str(e))
                        print info_str, "\n...shutting down."
                        os._exit(1)

                self.logger.info('Saved file %s, GOOD.', filename)

                if not self.visitonly:  # spider will parse RDF in page

                    # Find RDF statements in page
                    try:
                        # TODO: maybe parse respose.body directly? instead of reading file?
                        # grrr = Graph().parse(data=response.body, format='html', publicID=response.meta["url4base"])
                        # print "\n\n\n\n\n\n{0}\n\n\n\n\n".format(grrr.serialize(format="turtle"))
                        graph4page = parse_graph(filename, response, response.meta["url4base"])
                    except Exception as e:
                        info_str = u"Ooops: {0}: {1}".format(e.__class__.__name__, str(e))
                        print info_str
                        traceback.print_exc(file=sys.stdout)
                        self.logger.error(info_str)

                    else:  # no exception during graph parsing
                        if graph4page:
                            graph_length = len(graph4page)
                            if not is_empty_parsed_graph(graph4page):
                                if self.debug:
                                    rdf_file = '_len{0}_RDF_graph{1}_{2}.json'.format(graph_length, self.page_counter, filename1)
                                    try:
                                        if not os.path.exists(self.crawled_rdf_dir):
                                            os.mkdir(self.crawled_rdf_dir)
                                        with open(os.path.join(self.crawled_rdf_dir, rdf_file), 'wb') as f:
                                            f.write(graph4page.serialize(format='json-ld'))
                                    except IOError as e:
                                        if e.errno == 63:
                                            filename1 = filename1[:100] + 'CUTSHORT'
                                            with open(os.path.join(self.crawled_rdf_dir, rdf_file), 'ab') as f:
                                                f.write(graph4page.serialize(format='json-ld'))
                                                # Add original page URL, for debugging purposes
                                                f.write('\n\n"trueurl" : "{0}"'.format(response.url))
                                        else:
                                            info_str = u"Ooops: {0}: {1}".format(e.__class__.__name__, str(e))
                                            print info_str
                                            traceback.print_exc(file=sys.stdout)
                                            self.logger.error(info_str)
                                self.hasRDF = True
                                self.pageWithRDF_counter += 1

                            # Run SPARQL query to find statatements using the CiTO ontology
                            query_result = run_query(graph4page, self.sparql_query)  # query_result is an instance of rdflib.plugins.sparql.processor.SPARQLResult

                            if query_result and len(query_result) > 0:
                                self.hasCiTOStatements = True
                                self.pageWithCiTOStatements_counter += 1
                                if self.debug:
                                    try:
                                        if not os.path.exists(self.crawled_cito_dir):
                                            os.mkdir(self.crawled_cito_dir)
                                        with open(os.path.join(self.crawled_cito_dir, 'CiTO_g_{0}_{1}.json'.format(self.page_counter, filename1)), 'wb') as f:
                                            f.write(query_result.serialize(format='json-ld'))
                                    except IOError as e:
                                        if e.errno == 63:
                                            filename1 = filename1[:100] + 'CUTSHORT'
                                            with open(os.path.join(self.crawled_cito_dir, 'CiTO_g_{0}_{1}.json'.format(self.page_counter, filename1)), 'ab') as f:
                                                f.write(graph4page.serialize(format='json-ld'))
                                                # Add original page URL, for debugging purposes
                                                f.write('\n\n"trueurl" : "{0}"'.format(response.url))
                                        else:
                                            info_str = u"Ooops: {0}: {1}".format(e.__class__.__name__, str(e))
                                            print info_str
                                            traceback.print_exc(file=sys.stdout)
                                            self.logger.error(info_str)

                                result_graph = query_result.graph
                                self.globalGraph += result_graph

                                try:
                                    graphset4page = GraphSet(base_iri, context_path, info_dir)

                                    # start populating graphset4page

                                    graph_subjects = result_graph.subjects()
                                    subjects_set = set(graph_subjects)

                                    for i, sub in enumerate(subjects_set):
                                        # if sub is not found in url2oc_dict, add it as a key and generate a new OC-compliant URI
                                        if self.url2oc_dict.get(sub) is None:
                                            self.resource_counter += 1
                                            oc_compliant_sub = self.url2oc_dict[sub] = URIRef(base_iri + 'br/' + str(self.resource_counter))
                                            res_par = None
                                        else:
                                            res_par = oc_compliant_sub = self.url2oc_dict[sub]

                                        # add all statements having sub as subject to a new graph having oc_compliant_sub as subject
                                        po = result_graph.predicate_objects(sub)
                                        graph4subject = Graph(identifier=base_iri + 'br/')

                                        # bind oc_compliant_sub to sub
                                        graph4subject.add((oc_compliant_sub, URIRef("http://purl.org/dc/terms/relation"), sub))
                                        for el in po:
                                            graph4subject.add((oc_compliant_sub, el[0], el[1]))

                                        # add graph4subject to the list of graphs in GraphSet
                                        graphset4page.g += [graph4subject]

                                        # create GraphEntity
                                        print "----->", graphset4page._generate_entity(
                                            graph4subject,
                                            res=res_par,
                                            res_type=URIRef("http://purl.org/spar/fabio/Expression"),
                                            resp_agent="CiTOCrawler",  # è il curator
                                            source=response.meta["url4base"],
                                            count=self.resource_counter,
                                            label=None,
                                            short_name="br"
                                        ), sub
                                    # Now grapheset4page is populated

                                    # Create ProvSet based on graphset4page
                                    prov = ProvSet(graphset4page, base_iri, context_path, info_dir,
                                                   ResourceFinder(base_dir=base_dir,
                                                                  base_iri=base_iri,
                                                                  tmp_dir=temp_dir_for_rdf_loading,
                                                                  context_map={context_path: context_file_path}))
                                    prov.generate_provenance()

                                    # Store graphset4page
                                    res_storer = Storer(graphset4page,
                                                        context_map={context_path: context_file_path},
                                                        dir_split=dir_split_number,
                                                        n_file_item=items_per_file)
                                    res_storer.upload_and_store(
                                        base_dir, triplestore_url, base_iri, context_path,
                                        temp_dir_for_rdf_loading)

                                    # Store provenance graph
                                    prov_storer = Storer(prov,
                                                         context_map={context_path: context_file_path},
                                                         dir_split=dir_split_number,
                                                         n_file_item=items_per_file)

                                    prov_storer.upload_and_store(base_dir, triplestore_url, base_iri, context_path, temp_dir_for_rdf_loading)

                                except Exception as e:
                                    info_str = u"Something went wrong during graph storing or uploading.\n"
                                    info_str += u"Ooops: {0}: {1}".format(e.__class__.__name__, str(e))
                                    traceback.print_exc(file=sys.stdout)
                                    self.logger.error(info_str)

                if self.debug:
                    # Delete saved page in case it does not contain any RDF statements
                    if not self.hasRDF:
                        os.remove(os.path.join(self.crawled_pages_dir, filename))
                else:
                    # Delete saved page in case it does not contain any CiTO statements
                    if not self.hasCiTOStatements:
                        os.remove(os.path.join(self.crawled_pages_dir, filename))

                # Decide whether or not to follow the links found in page, depending on spider initialization values

                # if xpath method returns an empty list, the following for loop is not executed
                for a in response.xpath("//a[not(starts-with(@href, '#'))]/@href"):  # excludes links to same page
                    # a is some Scrapy object. a.extract() returns the href attribute value
                    next_page = response.urljoin(a.extract())  # creates absolute URL
                    if not next_page.startswith('http'):  # scheme is mailto, ftp, etc. Anything but http(s)
                        info_str = u'Ignoring {0}: unwanted scheme'.format(next_page)
                        self._print_and_log(info_str)
                    else:
                        last_crawl_time = self.crawled_urls_dict.get(next_page)
                        if last_crawl_time and self._data_still_valid(last_crawl_time):
                            info_str = u"Ignoring URL: crawled {0} recently.".format(next_page)
                            self._print_and_log(info_str)
                        else:  # page has never been crawled or was crawled more than self.maxhours ago

                            if (self.samedomain is True) and (urlsplit(response.meta["url4base"])[1] not in next_page):
                                # info: value at index 1 in tuple returned by urlsplit() stores network location ( [wiki.]something.com )
                                info_str = u'Ignoring {0}: domain is different from {1}'.format(next_page, response.meta["url4base"])
                                self._print_and_log(info_str)
                            else:
                                if self.allowedlinks and (not any(re.compile(el).search(next_page) for el in self.allowedlinks)):
                                    # https: // docs.python.org / 2 / library / re.html  # match-objects
                                    # Match objects always have a boolean value of True.
                                    # Since match() and search() return None when there is no match,
                                    # you can test whether there was a match with a simple if statement
                                    info_str = u'Ignoring {0}: not matching any given regex in set: {1}'.format(next_page, ', '.join(self.allowedlinks))
                                    self._print_and_log(info_str)
                                else:
                                    yield response.follow(a, callback=self.parse, meta={"url4base": next_page})

    def _data_still_valid(self, last_crawl_time):
        last = datetime.strptime(last_crawl_time, '%Y-%m-%dT%H:%M:%SZ')
        now = datetime.now()
        elapsed_time = now - last
        # total_seconds returns a float
        diff_hours = elapsed_time.total_seconds() / 3600
        if diff_hours <= self.maxhours:
            return True
        else:
            return False

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(CitoSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        self._generate_statistics()
        # spider.logger.info('Spider closed: %s', spider.name)

    def _generate_statistics(self):

        # Calculate elapsed time
        total_time = (self.crawler.stats.get_value('finish_time') - self.crawler.stats.get_value('start_time'))
        d = total_time.days
        h, remainder = divmod(total_time.seconds, 3600)
        m, s = divmod(remainder, 60)
        time_str = "" if total_time.total_seconds() > 1 else "less than 1 second."
        time_str += "{0} day{1}, ".format(d, "s" if d > 1 else "") if d > 0 else ""
        time_str += "{0} hour{1}, ".format(h, "s" if h > 1 else "") if h > 0 else ""
        time_str += "{0} minute{1}, ".format(m, "s" if m > 1 else "") if m > 0 else ""
        time_str += "{0} second{1}.".format(s, "s" if s > 1 else "") if s > 0 else ""

        if self.visitonly:
            print "Total time running:", time_str
        else:
            try:
                with open(os.path.join(self.main_dir, 'CiTOGraph.json'), 'w') as f:
                    f.write(self.globalGraph.serialize(format='json-ld'))
            except Exception as e:
                info_str = u"Ooops: {0}: {1}".format(e.__class__.__name__, str(e))
                traceback.print_exc(file=sys.stdout)
                self.logger.error(info_str)

            stats = ''
            stats += "Some statistics on the last crawl:\n\n"
            stats += 'Visited pages: {0}\n'.format(self.page_counter)
            stats += 'Visited pagesWithRDF: {0}\n'.format(self.pageWithRDF_counter)
            stats += 'Visited pagesWithCiTOStatements: {0}\n\n'.format(self.pageWithCiTOStatements_counter)
            stats += 'Total time running: {0}\n\n'.format(time_str)
            #
            # # no. il problema è il denominatore che non può essere 0
            # # quando può essere 0 self.pageWithRDF_counter? se non trova rdf.
            # #       ma se non trova rdf non trova manco cito, quindi 0/0
            # # ?
            # # e quando può essere zero self.page_counter?
            # if self.pageWithCiTOStatements_counter == 0:
            #     avg1 = avg2 = 0
            # else
            # avg1 = (float(self.pageWithCiTOStatements_counter) / float(self.pageWithRDF_counter))
            # avg2 = (float(self.pageWithCiTOStatements_counter) / float(self.page_counter))

            if self.page_counter > 0:
                avg1 = float(self.pageWithCiTOStatements_counter) / float(self.page_counter)
                stats += 'Average1 {:40}: {}\n'.format("(#pagesWithCiTOStatements/#pages)", avg1)
                if self.pageWithRDF_counter > 0:
                    avg2 = (float(self.pageWithCiTOStatements_counter) / float(self.pageWithRDF_counter))
                    stats += 'Average2 (#pagesWithCiTOStatements/#pagesWithRDF): {0}\n'.format(avg2)
            if self.globalGraph:
                # Find most popular predicates
                q = """
                    SELECT ?p (COUNT(?p) as ?NPREDICATES)
                    WHERE {
                      ?s ?p ?o .
                    }
                    GROUP BY ?p
                    ORDER BY DESC(COUNT(?p)) ASC(?p)
                    """
                query_result = self.globalGraph.query(q)
                stats += "\n{:>17}  {:}\n".format("OCCURENCES", "PREDICATE")
                for i, row in enumerate(query_result):
                    (p, n) = row
                    stats += "{:4d}  {:>10d}:  {}\n".format(i + 1, int(n), str(p)[26:])

            print stats
            try:
                with open(os.path.join(self.main_dir, 'stats.txt'), 'w') as f:
                    f.write(stats)
            except Exception as e:
                info_str = u"Ooops: {0}: {1}".format(e.__class__.__name__, str(e))
                traceback.print_exc(file=sys.stdout)
                self.logger.error(info_str)

    def _print_and_log(self, message):
        print message
        self.logger.info(message)
