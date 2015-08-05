#!/bin/bash
set -e

if [ $# -lt 2 ]; then
 echo "Usage: $0 <httplog.in> <out>"
 exit -1
fi

httplog=$1
output=$2-`date +%y%m%d-%H%M%S`
echo "Output file:" $output

# build with java 6 to be compatible with our Spark cluster
sbt -java-home $(/usr/libexec/java_home -v '1.6*') assembly

APP_NAME="cn.edu.sjtu.omnilab.cobac.ExtractUrlFromHttp"
BINJAR="target/scala-2.10/cobac-extracturls-assembly-1.0.jar"

spark-submit --master local --class $APP_NAME $BINJAR $httplog $output

if [ -d $output ]; then
    echo "Output: "
    head -n 20 $output/part-00000
    echo "..."
fi