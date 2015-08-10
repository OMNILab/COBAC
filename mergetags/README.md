Merge tagged httplog and wifilog
================================

Build
-----

    $ sbt assembly

The binary jar is located in `target/scala-2.10/cobac-mergetags-assembly-1.0.jar`

Spark Jobs
----------

There are two jobs to generate the merged HTTP data from multiple sources:

* `cn.edu.sjtu.omnilab.cobac.CleanseWifilogJob`: this job mangles the raw wifi syslog and generates user sessions.
* `cn.edu.sjtu.omnilab.cobac.MergeHttpAndWifi`: this job merge the cleansed http data and wifi syslog to generate a unified dataset.

How to Use
----------

* `CleanseWifilogJob`:

        spark-submit --master yarn --class cn.edu.sjtu.omnilab.cobac.CleanseWifilogJob \
            cobac-mergetags-assembly-1.0.jar \
            <rawwifisyslog> <output>

* `MergeHttpAndWifi`:

        spark-submit --master yarn --class cn.edu.sjtu.omnilab.cobac.MergeHttpAndWifi \
            cobac-mergetags-assembly-1.0.jar \
            <httplog> <wifilog> <output>