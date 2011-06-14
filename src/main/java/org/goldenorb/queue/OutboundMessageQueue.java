package org.goldenorb.queue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.goldenorb.Message;
import org.goldenorb.Messages;
import org.goldenorb.OrbPartitionCommunicationProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutboundMessageQueue {
	 private Logger omqLogger;
	
	 List<Map<String, List<Message<? extends Writable>>>> partitionMessageMap;
	 private int numberOfPartitions;
	 private List<Integer> partitionMessageCounter;
	 private int maxMessages;
	 private Map<Integer, OrbPartitionCommunicationProtocol> orbClients;
	 private Class<? extends Message<? extends Writable>> messageType;
	 private int partitionId;
	 
	 public OutboundMessageQueue(int numberOfPartitions, int maxMessages, Map<Integer, OrbPartitionCommunicationProtocol> orbClients2, Class<? extends Message<? extends Writable>> messageType, int partitionId){
		 omqLogger = LoggerFactory.getLogger(OutboundMessageQueue.class);
		 
		 this.partitionId = partitionId;
		 this.numberOfPartitions = numberOfPartitions;
		 this.maxMessages = maxMessages;
		 this.orbClients = orbClients2;
		 this.messageType = messageType;
		 
		 partitionMessageMap = new ArrayList<Map<String, List<Message<? extends Writable>>>>(numberOfPartitions);
		 for(int i = 0; i < numberOfPartitions; i++){
			 partitionMessageMap.add(Collections.synchronizedMap(new HashMap<String, List<Message<? extends Writable>>>()));
		 }
		 
		 partitionMessageCounter = new ArrayList<Integer>(numberOfPartitions);
		 for(int i=0; i < numberOfPartitions; i++){
			 partitionMessageCounter.add(0);
		 }
	 }
	 
	 public void sendMessage(Message<? extends Writable> m)
	 {
		 int messageHash = Math.abs(m.getDestinationVertex().hashCode()) % numberOfPartitions ;
		 Map<String, List<Message<? extends Writable>>> currentPartition = partitionMessageMap.get(messageHash);
		 Integer messageCounter;
		 synchronized(partitionMessageCounter){
			 synchronized(currentPartition){
			 messageCounter = partitionMessageCounter.get(messageHash);
			 messageCounter++;
			 partitionMessageCounter.set(messageHash, messageCounter);
		 }
		 
		 if(currentPartition.containsKey(m.getDestinationVertex())){
			 currentPartition.get(m.getDestinationVertex()).add(m);
		 }
		 else {
			 List<Message<? extends Writable>> messageList = Collections.synchronizedList(new ArrayList<Message<? extends Writable>>());
			 messageList.add(m);
			 currentPartition.put(m.getDestinationVertex(), messageList);
		 }
		 
		 if (messageCounter >= maxMessages){
					 Messages messagesToBeSent = new Messages(messageType);
					 
					 for(Collection<Message<? extends Writable>> ms: currentPartition.values()){
						 for(Message<? extends Writable> message: ms){
							 messagesToBeSent.add(message);
						 }
					 }
					 omqLogger.info(this.toString() + " Partition: " + Integer.toString(partitionId)+ "Sending bulk messages. Count: " + messageCounter + ", " + messagesToBeSent.size());
					 omqLogger.info(messageType.getName());
					 orbClients.get(messageHash).sendMessages(messagesToBeSent);
					 partitionMessageMap.set(messageHash, Collections.synchronizedMap(new HashMap<String, List<Message<? extends Writable>>>()));
					 partitionMessageCounter.set(messageHash, new Integer(0));
			 }
		 }
	 }
	 
	 public void sendRemainingMessages(){
			 
		 for(int partitionID = 0; partitionID < numberOfPartitions; partitionID++){
			 Messages messagesToBeSent = new Messages(messageType);
			 
			 for(Collection<Message<? extends Writable>> ms: partitionMessageMap.get(partitionID).values()){
				 for(Message<? extends Writable> message: ms){
					 messagesToBeSent.add(message);
				 }
			 }
			 omqLogger.info("Sending bulk messages.  Count: " + partitionMessageCounter.get(partitionID) + ", " + messagesToBeSent.size());
			 orbClients.get(partitionID).sendMessages(messagesToBeSent);
		 } 
	 }
}
