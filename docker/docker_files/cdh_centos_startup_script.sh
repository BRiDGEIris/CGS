#!/bin/bash

export JAVA_HOME="/usr/lib/jvm/java"

echo "Start HDFS"
bash -c 'for x in `cd /etc/init.d ; ls hadoop-hdfs-*` ; do service $x start ; done'

echo "Start Yarn"
service hadoop-yarn-resourcemanager start
service hadoop-yarn-nodemanager start
service hadoop-mapreduce-historyserver start
#Satish: Don't start oozie 
#service oozie start

echo "Start Components"
service hue start

service zookeeper-server init

nohup hiveserver2 &

bash -c 'for x in `cd /etc/init.d ; ls impala-*` ; do service $x start ; done'

rm -rf /tmp/hbase-hbase*

service hbase-master start
service hbase-thrift start

