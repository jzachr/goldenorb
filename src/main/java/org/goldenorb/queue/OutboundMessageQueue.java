package org.goldenorb.queue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.goldenorb.Message;
import org.goldenorb.Messages;
import org.goldenorb.OrbPartitionCommunicationProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutboundMessageQueue {
  private Logger omqLogger;
  
  private int numberOfPartitions;
  private int maxMessages;
  public Map<Integer,OrbPartitionCommunicationProtocol> orbClients;
  private Class<? extends Message<? extends Writable>> messageClass;
  private int partitionId;
  
  PartitionMessagingObject pmo;
  
  public OutboundMessageQueue(int numberOfPartitions,
                              int maxMessages,
                              Map<Integer,OrbPartitionCommunicationProtocol> orbClients,
                              Class<? extends Message<? extends Writable>> messageClass,
                              int partitionId) {
    omqLogger = LoggerFactory.getLogger(OutboundMessageQueue.class);

    this.numberOfPartitions = numberOfPartitions;
    this.maxMessages = maxMessages;
    this.orbClients = orbClients;
    this.messageClass = messageClass;
    this.partitionId = partitionId;
    
    List<Map<String,List<Message<? extends Writable>>>> partitionMessageMapsList;
    List<Integer> partitionMessageCounter;
    
    // creates a HashMap<Vertex,Message List> for each outbound partition
    partitionMessageMapsList = new ArrayList<Map<String,List<Message<? extends Writable>>>>(
        numberOfPartitions);
    for (int i = 0; i < numberOfPartitions; i++) {
      partitionMessageMapsList.add(Collections
          .synchronizedMap(new HashMap<String,List<Message<? extends Writable>>>()));
    }
    
    // initializes a message counter for each outbound partition
    partitionMessageCounter = new ArrayList<Integer>(numberOfPartitions);
    for (int i = 0; i < numberOfPartitions; i++) {
      partitionMessageCounter.add(0);
    }
    
    this.pmo = new PartitionMessagingObject(partitionMessageMapsList, partitionMessageCounter);
  }
  
  public void sendMessage(Message<? extends Writable> m) {
    synchronized (pmo) {
      // get the HashMap bin that is unique to the DestinationVertex
      int messageHash = Math.abs(m.getDestinationVertex().hashCode()) % numberOfPartitions;
      Map<String,List<Message<? extends Writable>>> currentPartition = pmo.partitionMessageMapsList.get(messageHash);
      
      Integer messageCounter;
      synchronized (pmo.partitionMessageCounter) {
        synchronized (currentPartition) {
          messageCounter = pmo.partitionMessageCounter.get(messageHash);
          messageCounter++; // increment the message counter
          pmo.partitionMessageCounter.set(messageHash, messageCounter);
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
          Messages messagesToBeSent = new Messages(messageClass);
          
          // collects the messages associated to each key and adds them to a Messages object to be sent
          for (Collection<Message<? extends Writable>> ms : currentPartition.values()) {
            for (Message<? extends Writable> message : ms) {
              messagesToBeSent.add(message);
            }
          }
          
          // logger stuff
          omqLogger
              .info(this.toString() + " Partition: " + Integer.toString(partitionId)
                    + "Sending bulk messages. Count: " + messageCounter + ", " + messagesToBeSent.size());
          omqLogger.info(messageClass.getName());
          
          // sends the Messages to the partition as specified over RPC, then creates a fresh, empty Map in its
          // place
          orbClients.get(messageHash).sendMessages(messagesToBeSent);
          pmo.partitionMessageMapsList.set(messageHash,
            Collections.synchronizedMap(new HashMap<String,List<Message<? extends Writable>>>()));
          pmo.partitionMessageCounter.set(messageHash, new Integer(0)); // reset counter to 0
        }
      }
    }
  }
  
  public void sendRemainingMessages() {
    
    for (int partitionID = 0; partitionID < numberOfPartitions; partitionID++) {
      Messages messagesToBeSent = new Messages(messageClass);
      
      for (Collection<Message<? extends Writable>> ms : pmo.partitionMessageMapsList.get(partitionID)
          .values()) {
        for (Message<? extends Writable> message : ms) {
          messagesToBeSent.add(message);
        }
      }
      omqLogger.info("Sending bulk messages.  Count: " + pmo.partitionMessageCounter.get(partitionID) + ", "
                     + messagesToBeSent.size());
      orbClients.get(partitionID).sendMessages(messagesToBeSent);
    }
  }
  
  class PartitionMessagingObject {
    
    List<Map<String,List<Message<? extends Writable>>>> partitionMessageMapsList;
    List<Integer> partitionMessageCounter;
    
    public PartitionMessagingObject(List<Map<String,List<Message<? extends Writable>>>> partitionMessageMapsList,
                                    List<Integer> partitionMessageCounter) {
      this.partitionMessageMapsList = partitionMessageMapsList;
      this.partitionMessageCounter = partitionMessageCounter;
    }
    
    public List<Map<String,List<Message<? extends Writable>>>> getMapsList() {
      return partitionMessageMapsList;
    }
    
    public List<Integer> getMessageCounter() {
      return partitionMessageCounter;
    }
    
  }
}
