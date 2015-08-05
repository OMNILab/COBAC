ExtractUrlFromHttp
===================

Prepare
-------

Install and configure Apache Spark for local test.


Build
-----

    sbt assembly

Run
---

    spark-submit --master local --class cn.edu.sjtu.omnilab.cobac.ExtractUrlFromHttp\
      target/scala-2.10/cobac-extracturls-assembly-1.0.jar \
      input output

Test script
-----------

    chmod +x bin/test_extract_urls.sh
    ./bin/test_extract_urls.sh

Enjoy!