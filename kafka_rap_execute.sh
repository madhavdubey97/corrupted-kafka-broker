#!/bin/bash


###############################################################
##This Script will execut the generated reassignment json file
## and will verify the progress of the reassignment


for i in `ls /tmp/re-partition/rap-json-execute/new`;
do
	/data/kafka_2.11-2.1.1/bin/kafka-reassign-partitions.sh --zookeeper localhost:2181  --bootstrap-server localhost:9092 --reassignment-json-file /tmp/re-partition/rap-json-execute/new/$i --execute
        sleep 5
	/data/kafka_2.11-2.1.1/bin/kafka-reassign-partitions.sh --zookeeper localhost:2181  --bootstrap-server localhost:9092 --reassignment-json-file /tmp/re-partition/rap-json-execute/new/$i --verify
	sleep 10
	/data/kafka_2.11-2.1.1/bin/kafka-reassign-partitions.sh --zookeeper localhost:2181  --bootstrap-server localhost:9092 --reassignment-json-file /tmp/re-partition/rap-json-execute/new/$i --verify
done
