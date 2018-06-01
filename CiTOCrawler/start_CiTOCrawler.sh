#!/usr/bin/env bash

python -u CiTO_crawl.py 2>&1 | tee CiTOCrawler_log_`date +%Y-%m-%d_%H-%M-%S`.txt
