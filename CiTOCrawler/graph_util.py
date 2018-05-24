# -*- coding: utf-8 -*-

from rdflib import Graph, URIRef
import traceback
import sys
import os
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def parse_ld_plus_json(scrapy_response, base):
    """
    Checks if page contains <script type="application/ld+json"> and saves RDF statements found in that tag
    :param scrapy_response: a Scrapy response object containing an HTML page
    :param base: a string that must be a valid URI
    :return: a rdflib.Graph or None (if graph could not be created)
    """
    if scrapy_response and base:
        g = Graph()
        try:
            # # pythonic version
            # script_content = scrapy_response.css('script[type="application/ld+json"]::text').extract_first()
            # # print script_content
            # g.parse(data=script_content, format='json-ld', publicID=base)

            # old non-pythonic version
            if len(scrapy_response.css('script[type="application/ld+json"]')) > 0:
                script_content = scrapy_response.css('script[type="application/ld+json"]::text').extract_first()
                # print script_content
                g.parse(data=script_content, format='json-ld', publicID=base)
        except Exception as e:
            info_str = u"Something went wrong in parse_ld_plus_json(): {0}: {1}".format(e.__class__.__name__, str(e))
            print info_str
            traceback.print_exc(file=sys.stdout)
            logger.exception(info_str)
            return None
        else:  # no exception
            return g
    else:
        return None


def parse_graph(filename, scrapy_response, base):
    """
    Looks for RDFa, Microdata, Turtle script tag, ld+json script tag in scrapy_response and returns the resulting graph
    :param filename: name of file with HTML code for some page
    :param scrapy_response: a Scrapy response object containing an HTML page
    :param base: a string that must be a valid URI
    :return: an rdflib.Graph instance or None (if graph could not be created)
    """
    g1 = Graph()  # g1 evaluates to False. len(g1) is 0. print g1 outputs "[a rdfg:Graph;rdflib:storage [a rdflib:Store;rdfs:label 'IOMemory']]."
    try:
        # parse RDFa, Microdata, turtle script inside page
        filepath = os.path.join(os.path.dirname(__file__), "crawled_pages", filename)
        g1.parse(filepath, format='html', publicID=base)
    except Exception as e:
        info_str = u"Something went wrong in parse_graph() with g1: {0}: {1}".format(e.__class__.__name__, str(e))
        print info_str
        traceback.print_exc(file=sys.stdout)
        logger.exception(info_str)

        # parse ld+json script tag
        g2 = parse_ld_plus_json(scrapy_response, base)
        return g2  # this is either None or a valid graph (possibly empty)

    else:  # g1 is a valid graph (possibly empty)
        g2 = parse_ld_plus_json(scrapy_response, base)
        if g2:  # g2 is a valid graph (possibly empty)
            g1 += g2  # g1 now contains g1 UNION g2
            return g1
        else:  # g2 is None or empty
            return g1


def is_empty_parsed_graph(graph):
    """
    Checks if graph parsed from web page only contains an "empty" statement, that was not embedded in page
    namely (<subjectURI>, <http://www.w3.org/ns/md#item>, <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>)
    :param graph: an rdflib.Graph
    :return: True if graph contains no "real" RDF, False otherwise
    """
    if len(graph) > 1:
        return False
    for po in graph.predicate_objects(None):
        if po == (URIRef(u'http://www.w3.org/ns/md#item'),
                  URIRef(u'http://www.w3.org/1999/02/22-rdf-syntax-ns#nil')):
            return True
        return False


def run_query(graph, q):
    """
    Run SPARQL query q on graph parameter
    :param graph: an rdflib.Graph
    :param q: string
    :return: an instance of rdflib.plugins.sparql.processor.SPARQLResult or None if any exceptions occurred
    """
    if graph and q:
        try:
            query_result = graph.query(q)
        except Exception as e:
            info_str = u"Something went wrong in run_query: {0}: {1}".format(e.__class__.__name__, str(e))
            print info_str
            traceback.print_exc(file=sys.stdout)
            logger.exception(info_str)

            return None
        else:
            return query_result
    else:
        return None

