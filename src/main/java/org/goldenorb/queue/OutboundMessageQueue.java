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
  
  List<Map<String,List<Message<? extends Writable>>>> partitionMessageMapsList;
  private int numberOfPartitions;
  private List<Integer> partitionMessageCounter;
  private int maxMessages;
  public Map<Integer,OrbPartitionCommunicationProtocol> orbClients;
  private Class<? extends Message<? extends Writable>> messageType;
  private int partitionId;
  
  public OutboundMessageQueue(int numberOfPartitions,
                              int maxMessages,
                              Map<Integer,OrbPartitionCommunicationProtocol> orbClients,
                              Class<? extends Message<? extends Writable>> messageType,
                              int partitionId) {
    omqLogger = LoggerFactory.getLogger(OutboundMessageQueue.class);
    
    this.partitionId = partitionId;
    this.numberOfPartitions = numberOfPartitions;
    this.maxMessages = maxMessages;
    this.orbClients = orbClients;
    this.messageType = messageType;
    
    // creates a HashMap<Vertex,Message List> for each outbound partition
    partitionMessageMapsList = new ArrayList<Map<String,List<Message<? extends Writable>>>>(numberOfPartitions);
    for (int i = 0; i < numberOfPartitions; i++) {
      partitionMessageMapsList.add(Collections
          .synchronizedMap(new HashMap<String,List<Message<? extends Writable>>>()));
    }
    
    // initializes a message counter for each outbound partition
    partitionMessageCounter = new ArrayList<Integer>(numberOfPartitions);
    for (int i = 0; i < numberOfPartitions; i++) {
      partitionMessageCounter.add(0);
    }
  }
  
  public void sendMessage(Message<? extends Writable> m) {
    
    // get the HashMap bin that is unique to the DestinationVertex
    int messageHash = Math.abs(m.getDestinationVertex().hashCode()) % numberOfPartitions;
    Map<String,List<Message<? extends Writable>>> currentPartition = partitionMessageMapsList.get(messageHash);
    Integer messageCounter;
    synchronized (partitionMessageCounter) {
      synchronized (currentPartition) {
        messageCounter = partitionMessageCounter.get(messageHash);
        messageCounter++; // increment the message counter
        partitionMessageCounter.set(messageHash, messageCounter);
      }
      
      // if Vertex exists as a key, add the Message
      // else create a new synchronized list for the key on demand, then put the list in the map
      if (currentPartition.containsKey(m.getDestinationVertex())) {
        currentPartition.get(m.getDestinationVertex()).add(m);
      } else {
        List<Message<? extends Writable>> messageList = Collections
            .synchronizedList(new ArrayList<Message<? extends Writable>>());
        messageList.add(m);
        currentPartition.put(m.getDestinationVertex(), messageList);
      }
      
      // once the expected number of messages is met, begins the message sending operation
      if (messageCounter >= maxMessages) {
        Messages messagesToBeSent = new Messages(messageType);
        
        // collects the messages associated to each key and adds them to a Messages object to be sent
        for (Collection<Message<? extends Writable>> ms : currentPartition.values()) {
          for (Message<? extends Writable> message : ms) {
            messagesToBeSent.add(message);
          }
        }
        
        // logger stuff
        omqLogger.info(this.toString() + " Partition: " + Integer.toString(partitionId)
                       + "Sending bulk messages. Count: " + messageCounter + ", " + messagesToBeSent.size());
        omqLogger.info(messageType.getName());
        
        // sends the Messages to the partition as specified over RPC, then creates a fresh, empty Map in its place
        orbClients.get(messageHash).sendMessages(messagesToBeSent);
        partitionMessageMapsList.set(messageHash,
          Collections.synchronizedMap(new HashMap<String,List<Message<? extends Writable>>>()));
        partitionMessageCounter.set(messageHash, new Integer(0)); // reset counter to 0
      }
    }
  }
  
  public void sendRemainingMessages() {
    
    for (int partitionID = 0; partitionID < numberOfPartitions; partitionID++) {
      Messages messagesToBeSent = new Messages(messageType);
      
      for (Collection<Message<? extends Writable>> ms : partitionMessageMapsList.get(partitionID).values()) {
        for (Message<? extends Writable> message : ms) {
          messagesToBeSent.add(message);
        }
      }
      omqLogger.info("Sending bulk messages.  Count: " + partitionMessageCounter.get(partitionID) + ", "
                     + messagesToBeSent.size());
      orbClients.get(partitionID).sendMessages(messagesToBeSent);
    }
  }
}
