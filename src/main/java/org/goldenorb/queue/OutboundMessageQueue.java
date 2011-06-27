/**
 * Licensed to Ravel, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Ravel, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

/**
 * This class constructs an OutboundMessageQueue which collects Messages to be sent out to other partitions
 * once a certain number has been collected.
 * 
 */
public class OutboundMessageQueue {
  private Logger omqLogger;
  
  private int numberOfPartitions;
  private int maxMessages;
  private Map<Integer,OrbPartitionCommunicationProtocol> orbClients;
  private Class<? extends Message<? extends Writable>> messageClass;
  private int partitionId;
  
  PartitionMessagingObject pmo;
  
  /**
   * This constructs the OutboundMessageQueue and creates the underlying data structures to be sent to a
   * PartitionMessagingObject.
   * 
   * @param numberOfPartitions
   *          - the number of partitions to be used in the Job
   * @param maxMessages
   *          - the maximum number of messages to be queued up before a send operation
   * @param orbClients
   *          - a Map of the communication protocol used by each Orb client
   * @param messageClass
   *          - the type of Message to be queued. User-defined by extension of Message
   * @param partitionId
   *          - the ID of the partition that creates and owns this OutboundMessageQueue
   */
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
  
  /**
   * This method queues up a Message to be sent. Once the Message count reaches the maximum number, it sends
   * the vertices via Hadoop RPC.
   * 
   * @param m
   *          - a Message to be sent
   */
  public void sendMessage(Message<? extends Writable> m) {
    synchronized (pmo) {
      // get the HashMap bin that is unique to the DestinationVertex
      int messageHash = Math.abs(m.getDestinationVertex().hashCode()) % numberOfPartitions;
      Map<String,List<Message<? extends Writable>>> currentPartition = pmo.partitionMessageMapsList
          .get(messageHash);
      
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
  
  /**
   * Sends any remaining messages if the maximum number of messages is not met.
   */
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
  
  /**
   * This inner class defines a PartitionMessagingObject, which is used strictly to encapsulate
   * partitionMessageMapsList and partitionMessageCounter into one object for synchronization purposes.
   * 
   */
  class PartitionMessagingObject {
    
    List<Map<String,List<Message<? extends Writable>>>> partitionMessageMapsList;
    List<Integer> partitionMessageCounter;
    
    /**
     * Constructor
     * 
     * @param partitionMessageMapsList
     * @param partitionMessageCounter
     */
    public PartitionMessagingObject(List<Map<String,List<Message<? extends Writable>>>> partitionMessageMapsList,
                                    List<Integer> partitionMessageCounter) {
      this.partitionMessageMapsList = partitionMessageMapsList;
      this.partitionMessageCounter = partitionMessageCounter;
    }
    
    /**
     * Return the mapsList.
     */
    public List<Map<String,List<Message<? extends Writable>>>> getMapsList() {
      return partitionMessageMapsList;
    }
    
    /**
     * Return the messageCounter.
     */
    public List<Integer> getMessageCounter() {
      return partitionMessageCounter;
    }
    
  }
}
