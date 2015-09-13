# cernet说明

---

## 包括四个文件: stat.py, stat.sh, airport.txt, ipdict.txt ##

 - stat.sh 为shell执行脚本
 需要将集群中源文件的位置作为参数传入，在目录下运行 source stat.sh "path" 即可
 - stat.py 为pyspark程序
 - airport.txt 为机场三字代码和地名的转换字典
 - ipdict.txt 为高校ip段和高校名之间的转换字典

## 输出 ##

将在目录下生成result文件夹，包括单7个维度的统计，以及若干两个维度的统计，为csv格式。
 


