#!/bin/bash

for i in {2014,2015}; do
  for j in {1..12}; do
    logname=`printf "wifilog%d-%02d" $i $j`
    input=/user/omnilab/warehouse/sjtu_wifi_syslog/$logname*
    output=sjtu_wifi_syslog/$logname
    echo $logname
    echo $input
    echo $output
    pig -e "dat = load '$input'; store dat into '$output';"
  done
done