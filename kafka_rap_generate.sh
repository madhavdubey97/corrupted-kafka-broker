#!/bin/bash

####This Scripting is generating the reassign partitions for the topics.json present under /tmp/re-partition/increase_rf/new/topics_ra this directory 
###if you don't have this path please create one
###please run below commands before running the script 
#### - mkdir -p /tmp/re-partition/increase_rf/new/topics_ra 
#### - mkdir -p /tmp/re-partition/rap-json-generate/new
#### - mkdir -p  /tmp/re-partition/rap-json-execute/new
#### Copy your topic name in every new line in /tmp/re-partition/increase_rf/topic_list.txt file
#### Copy sample_topics-to-move.json from git under /tmp/re-partition/increase_rf directory



##this loop is creating topic_name.json to be passed with --topics-to-move-json-file
for i in `cat /tmp/re-partition/increase_rf/topics_list.txt` ; do sed "s/SAMPLE/${i}/" /tmp/re-partition/increase_rf/sample_topics-to-move.json > /tmp/re-partition/increase_rf/new/topics_ra/${i}_move.json ; done   


#######################################################
##Generating reassignment of partitions executable under /tmp/re-partition/rap-json-execute/new directory
for i in `ls /tmp/re-partition/increase_rf/new/topics_ra`;
do 
	
       /data/kafka_2.11-2.1.1/bin/kafka-reassign-partitions.sh --zookeeper localhost:2181 --bootstrap-server localhost:9092 --topics-to-move-json-file /tmp/re-partition/increase_rf/new/topics_ra/$i --broker-list "100,143,144,110,120" --generate > /tmp/re-partition/rap-json-generate/new/$i-rap-generate.json	
      grep -A 1 Proposed /tmp/re-partition/rap-json-generate/new/$i-rap-generate.json | tail -1 > /tmp/re-partition/rap-json-execute/new/$i-rap-execute.json
      /bin/cat /tmp/re-partition/rap-json-execute/new/$i-rap-execute.json
done
