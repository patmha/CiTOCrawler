#!/usr/bin/env bash

nohup java -server -Xmx1G -Dbigdata.propertyFile=occ.properties -Djetty.port=3000 -Djetty.host=127.0.0.1 -jar blazegraph.jar &