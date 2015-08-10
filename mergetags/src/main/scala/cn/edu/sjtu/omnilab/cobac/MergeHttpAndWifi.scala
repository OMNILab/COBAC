package cn.edu.sjtu.omnilab.cobac

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

case class HttpLog(ip: String, time: Long, duration: String,
                   bytes: String, url: String, userAgent: String, tag:String,
                   contentType: String, user: String, building: String)

object MergeHttpAndWifi {
  final val movementToleranceMinutes = 5

  def main( args: Array[String] ): Unit = {

    if (args.length < 3) {
      println("Usage: MergeHttpAndWifi <httplog> <wifilog> <output>")
      sys.exit(0)
    }

    val httplog = args(0)
    val wifilog = args(1)
    val output = args(2)

    val conf = new SparkConf()
      .setAppName("Merge tagged http items and wifilog")
    val spark = new SparkContext(conf)

    // read movement data from clean wifi log
    // the timestamp in wifilog is in milliseconds (Long)
    val movRDD = spark.textFile(wifilog).map { m => {
      val parts = m.split(',')
      WIFISession(parts(0), parts(1).toLong, parts(2).toLong, parts(3), parts(4), parts(5))
    }
    }.keyBy(m => (m.IP, m.stime / 1000 / 3600 / 24))
      .groupByKey()
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // read clean and tagged http logs
    // the timestamp in httplog is in seconds (Double)
    val httpRDD = spark.textFile(httplog)
      .map(l => {
        val parts = l.split('|')
        val ip = parts(0)
        val time = parts(1)
        if ( ip == "N/A" || time == "N/A" || parts.length < 8)
          null
        else
          HttpLog(parts(0), (parts(1).toDouble * 1000).toLong, parts(2), parts(3),
                  parts(4), parts(5), parts(6), parts(7), null, null)
    }).filter(_ != null)
      .keyBy(m => (m.ip, m.time / 1000 / 3600 / 24))
      .groupByKey()

    // join wifi traffic and movement data
    val joinedRDD = httpRDD.join(movRDD)
      .flatMap { case (key, (http, movs)) => {

      val ordered = movs.toArray.sortBy(_.stime)
      var mergedLogs = new Array[HttpLog](0)

      // find movement session that contains the HTTP log
      // TODO: this can be optimized
      http.foreach { l => {

        var movFound: WIFISession = null
        // exact match
        ordered.foreach { mov => {
          if (movFound == null && l.time >= mov.stime && l.time <= mov.etime)
            movFound = mov
        }}

        if (movFound == null) {
          // fuzzy match
          ordered.foreach { mov => {
            val stime_ex = mov.stime - movementToleranceMinutes * 60 * 1000
            val etime_ex = mov.etime + movementToleranceMinutes * 60 * 1000

            if (movFound == null && l.time >= stime_ex && l.time <= etime_ex)
              movFound = mov
          }}
        }

        var combined: HttpLog = null
        if (movFound != null) {
          // create new contextual log
          mergedLogs = mergedLogs :+ HttpLog(
            ip = l.ip, time = l.time, duration = l.duration, bytes = l.bytes,
            url = l.url, userAgent = l.userAgent, tag = l.tag, contentType = l.contentType,
            user = movFound.account, building = movFound.AP)
        }

      }}

      mergedLogs.toIterable

    }
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)

    joinedRDD.sortBy(m => (m.user, m.time)).map(m => {
      "%s|%d|%s|%s|%s|%s|%s|%s|%s|%s".format(
        m.ip, m.time, m.duration, m.bytes, m.url,
        m.userAgent, m.tag, m.contentType, m.user, m.building)
    }).saveAsTextFile(output)

    spark.stop()
  }
}
