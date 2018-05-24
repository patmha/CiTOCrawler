#!/usr/bin/env bash

python CiTO_crawl.py 2>&1 | tee CiTOCrawler_log_`date +%Y%m%d%H%M%S`.txt
