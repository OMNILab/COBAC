from pyspark import SparkConf, SparkContext
import sys
import re

reload(sys)
sys.setdefaultencoding('utf-8')

APP_NAME = "FEATURES_FROM_HTTP"

def cleanse0(text):
        text = text.split("|")
        return (text[0],text[1])
def cleanse(text):
        chops = text.split("\" \"")
        if len(chops)!=21:
                return None
        timestamps = chops[0].split(" ")
        if len(timestamps) != 18:
                return None
        results = timestamps + chops[1:21]
        results = map(lambda x: None if x=="\"N/A\"" else x.strip("\""), results)
        return results
def hasProtoPrefix(url):
        if re.match(r"^(\\w+:?//).*", url):
                return True
        return False
def combineHostUri(text):
        host = text[20]
        url = text[18]
        if hasProtoPrefix(url):
                text[18] = url
        else:
                text[18] = host+url
        return text
def stripUrlProto(text):
        url = text[18]
        match = protopattern.match(url)
        if match:
                text[20] = match.group(2)
        else:
                text[20] = url
        return text
def stripUrlParam(text):
        url = text[20]
        match = parampattern.match(url)
        if match:
                text[20] = match.group(1)
        return text
def tagging(text):
        try:
                text[20] = tag[text[20]]
        except KeyError:
                text[20] = None
        return text
def main(sc,path):
        global protopattern, parampattern,tag
        protopattern = re.compile("^(\\w+:?//)?(.*)$", re.I)
        parampattern = re.compile("^((\\w+://)?([^\\?&]+))\\??", re.I)
        tag = sc.textFile("hdfs://namenode.omnilab.sjtu.edu.cn/user/cenkai/tag/"+path).map(cleanse0).collectAsMap()
        cleanhttp = sc.textFile("hdfs://namenode.omnilab.sjtu.edu.cn/user/omnilab/warehouse/sjtu_wifi/"+path+"/http/").map(cleanse)
        exthttp = cleanhttp.filter(lambda x: x!=None and x[18]!=None and x[20]!=None).map(combineHostUri).map(lambda x: stripUrlParam(stripUrlProto(x))).map(tagging)
        final = exthttp.map(lambda text : "%s|%s|%s|%d|%s|%s|%s|%s"%(text[0],text[5],text[7], int(text[15])+int(text[16]), text[18], text[21], text[20], text[29]))
        final.saveAsTextFile('tagged/'+path)

if __name__ == "__main__":
        conf = SparkConf().setAppName(APP_NAME)
        sc = SparkContext(conf=conf)
        path = sys.argv[1]
        main(sc,path)

