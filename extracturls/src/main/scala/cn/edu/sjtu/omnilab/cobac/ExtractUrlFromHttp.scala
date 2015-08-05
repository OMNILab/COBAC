package cn.edu.sjtu.omnilab.cobac

import java.util.regex.{Matcher, Pattern}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.omg.CORBA.PERSIST_STORE

object ExtractUrlFromHttp {

  final val httpProtoPattern = Pattern.compile("^(\\w+:?//)?(.*)$", Pattern.CASE_INSENSITIVE)
  final val httpParamPattern = Pattern.compile("^((\\w+://)?([^\\?&]+))\\??", Pattern.CASE_INSENSITIVE)

  def main( args: Array[String] ): Unit = {

    // parse command options
    if (args.length < 2){
      println("Usage: ExtractUrlFromHttp <HTTPLOG> <OUTPUT>")
      sys.exit(0)
    }

    val httplog = args(0)
    val output = args(1)

    // configure spark
    val conf = new SparkConf()
      .setAppName("Extracting unique urls from http logs")
    val spark = new SparkContext(conf)

    // extract fields from raw logs and validate input data
    val allUrlRDD = spark.textFile(httplog).map(cleanseLog(_))
      .filter( line => line != null &&
        line(DataSchema.request_host) != null &&
        line(DataSchema.request_url) != null)
      .map( line => {
        val url = combineHostUri(line(DataSchema.request_host), line(DataSchema.request_url))
        val curl = stripUrlParam(stripUrlProto(url))
        (curl, url)
      }).groupBy(_._1)
      .map { case (curl, pairs) => {
        val tnum = pairs.size
        if (tnum > 500 ) { // about TOP-60K in one month
          val oneurl = pairs.toList(1)._2
          (tnum, oneurl)
        } else {
          null
        }
      }}.filter(m => m != null)
      .sortByKey(false)
      .zipWithIndex
      .map { case ((tnum, url), index) => "%d|%d|%s".format(index, tnum, url) }
      .saveAsTextFile(output)

    spark.stop()
    
  }

  def cleanseLog(line: String): Array[String] = {
    // get HTTP header fields
    val chops = line.split("""\"\s\"""");
    if ( chops.length != 21 )
      return null

    // get timestamps
    val timestamps = chops(0).split(" ");
    if (timestamps.length != 18 )
      return null

    val results = timestamps ++ chops.slice(1, 21)

    // remove N/A values and extrat quote
    results.transform( field => {
      var new_field = field.replaceAll("\"", "")
      if (new_field == "N/A")
        new_field = null
      new_field
    })

    results
  }

  def hasProtoPrefix(uri: String): Boolean = {
    if ( uri.matches("^(\\w+:?//).*"))
      return true
    return false
  }

  def combineHostUri(host: String, uri: String): String = {
    if ( hasProtoPrefix(uri) ){
      return uri;
    } else {
      return host+uri;
    }
  }

  def stripUrlProto(url: String): String = {
    val matcher = httpProtoPattern.matcher(url);
    if ( matcher.find() ){
      return matcher.group(2);
    } else {
      return url;
    }
  }

  def stripUrlParam(url: String): String = {
    val matcher = httpParamPattern.matcher(url);
    if ( matcher.find() ){
      return matcher.group(1);
    } else {
      return url;
    }
  }


}