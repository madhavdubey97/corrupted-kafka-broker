##Mainly in this python Script we are removing one particular broker as a Leader and
##as well as from ISR to, so that we can remove that broker from the cluster.

import kafka
from kafka.cluster import ClusterMetadata
from kazoo.client import KazooClient
import random
import json
from bisect import bisect_left ,bisect
from  collections import OrderedDict
brokers=[110, 120, 143, 144]             #List of all the broker id except the one which we want to remove from the cluster
remove_broker=100                        #broker id of the broker which we want to remove from the cluster



def chooseLeader(isr):                   #This function is to choose the new Leader for the partitions
    tmp=isr[:]                           
    tmp.remove(remove_broker)            #Removing the brokerid from the ISR which we want to discard
    if tmp:
        leader=random.choice(tmp)        #if RF > 1 we are choosing leader Randomly from the ISR
        return leader
    else:
        leader=random.choice(brokers)    #if RF = 1 we are choosing randomly from the available brokers
        return leader



def uniqueBrokerIds(isr,brokers):        #This function is to find out the Unique broker ids from 
    unique_list = [] 			 #the ISR and the brokers List being used while creating new ISR
    for x in brokers:                    
        count=0
        for y in isr:
            if x != y:
                count=count+1
        if count == len(isr):
            unique_list.append(x)
    return unique_list

def createIsr(isr, leader):                            #This function is to create ISR
    isr.remove(remove_broker)                          
    unique_list=uniqueBrokerIds(isr, brokers)          #taking out available unique broker ids from ISR and brokers list
    if isr:
        if unique_list:                                #If RF > 1 appending the unique brokerids in the ISR
            replica=random.choice(unique_list)
            isr.append(replica)
            return isr
        else:
            return isr
    else:                                              #If RF = 1 appending the Leader in ISR
        isr.append(leader)
        return isr

def getPartitionState(path, leader, isr):                                     #This function is getting the actual state of the partitions from the zookeeper
    result=zk.get(path)                          			      
    state=result[0]
    print(state)
    state_dict=json.loads(state,object_pairs_hook=OrderedDict)
    state_dict['leader']=leader                                               #From existing partition state we are setting new Leader and New ISR in the state
    leader_epoch=state_dict['leader_epoch']				      #as well as we have to increase the leader_epoch by 1 while changing the leader of partiton
    leader_epoch=leader_epoch+1
    state_dict['leader_epoch']=leader_epoch
    up_dict={"isr": isr}
    state_dict.update(up_dict)
    state_str=json.dumps(state_dict)
    return state_str


zk = KazooClient(hosts='localhost:2181')                                       #zookeeper client
zk.start()
consumer_client = kafka.KafkaConsumer(bootstrap_servers=['localhost'])         #consumer kafka client
kfk_client=kafka.client.KafkaClient(bootstrap_servers=['localhost:9092'])
topic_set=consumer_client.topics()
topic_list=list(topic_set)                                                     #getting all topics list
cluster_metadata=ClusterMetadata(bootstrap_servers=['localhost'])
brokerMetadata=cluster_metadata.brokers()
partition=kfk_client.cluster._partitions 
topic_list.remove('-')

for topic in topic_list:
    count=0
    print(topic)
    for i in range(len(partition[topic])):				      			#Iterating on all topics
        isr_set=set(partition[topic][i].isr) 				
        if partition[topic][i].leader == remove_broker or remove_broker in isr_set:		#checking if the partiton leader or the ISR contains the same brokerid 
            part_num=str(i)
            path="/brokers/topics/"+topic+"/partitions/"+part_num+"/state"                      
            print(path)
            leader=chooseLeader(partition[topic][i].isr)
            isr=createIsr(partition[topic][i].isr, leader)
            part_state=getPartitionState(path, leader, isr)
            data=part_state
            print(data.replace(" ",""))
            zk.set(path, data.replace(" ",""))
            count=count+1
zk.stop()
