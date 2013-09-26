#!/bin/sh
cp="./target/TwitCrawl-0.1.1-SNAPSHOT-jar-with-dependencies.jar"
java="/usr/java/latest/bin/java"

#echo "$@" >> job.log
echo "starting crawl of $@: $(date)" >> job.log
$java -Xms1g -Xmx20g -classpath $cp de.hpi.fgis.YQLDumpFileCrawler "$@" >> progress.log 2> error.log
echo "crawl of $@ finished: $(date)" >> job.log
