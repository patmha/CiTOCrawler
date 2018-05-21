#!/usr/bin/env bash

# removes all files generated by CiTO_crawl.py except for the "CiTO_graphs" directory (which contains the OpenCitations-compliant graphs)

cd CiTOCrawler
rm crawledPages.csv
rm -r crawled_pages
rm -r crawled_rdf
for f in CiTO_g_*.json; do rm "$f"; done
rm CiTOGraph.json
rm stats.txt
