
from pyspark import SparkConf, SparkContext
import sys
import re

reload(sys)
sys.setdefaultencoding('utf-8')

APP_NAME = "TAGS"

def cleanse(line):
        line = line.split(" ")
        if len(line)!= 3:
                return None
        return (line[0], line[2])
def stripUrlProto(url):
        match = protopattern.match(url)
        if match:
                return match.group(2)
        else:
                return url
def stripUrlParam(url):
        match = parampattern.match(url)
        if match:
                return match.group(1)
        return url
def cleanse2(line):
        line = line.split("|")
        if len(line)!= 3:
                return None
        try:
                return [stripUrlParam(stripUrlProto(line[2])),cleantag[line[0]].replace("|",",")]
        except KeyError:
                return None
def main(sc,path):
        global cleantag
        global protopattern, parampattern
        protopattern = re.compile("^(\\w+:?//)?(.*)$", re.I)
        parampattern = re.compile("^((\\w+://)?([^\\?&]+))\\??", re.I)
        cleantag = sc.textFile("hdfs://namenode.omnilab.sjtu.edu.cn/user/omnilab/warehouse/sjtu-wifi-urls-tags/urlstat" + path + "/*.result").map(cleanse).collectAsMap()
        urltag = sc.textFile("hdfs://namenode.omnilab.sjtu.edu.cn/user/omnilab/warehouse/sjtu-wifi-urls-tags/urlstat" + path + "/*.txt").map(cleanse2)
        final = urltag.filter(lambda x : x!=None).map(lambda x : "%s|%s"%(x[0],x[1]))
        final.saveAsTextFile('tag/'+path)

if __name__ == "__main__":
        conf = SparkConf().setAppName(APP_NAME)
        sc = SparkContext(conf=conf)
        path = str(sys.argv[1])
        main(sc,path)