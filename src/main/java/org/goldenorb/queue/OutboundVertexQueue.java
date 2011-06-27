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
import java.util.List;
import java.util.Map;

import org.goldenorb.OrbPartitionCommunicationProtocol;
import org.goldenorb.Vertex;
import org.goldenorb.Vertices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class constructs an OutboundVertexQueue which collects Vertices to be sent out to other partitions
 * once a certain number has been collected.
 * 
 */
public class OutboundVertexQueue {
  private Logger ovqLogger;
  
  private int numberOfPartitions;
  private int maxVertices;
  private Map<Integer,OrbPartitionCommunicationProtocol> orbClients;
  private Class<? extends Vertex<?,?,?>> vertexClass;
  private int partitionId;
  
  PartitionVertexObject pvo;
  
  /**
   * This constructs the OutboundVertexQueue and creates the underlying data structures to be sent to a
   * PartitionVertexObject.
   * 
   * @param numberOfPartitions
   *          - the number of partitions to be used in the Job
   * @param maxVertices
   *          - the maximum number of vertices to be queued up before a send operation
   * @param orbClients
   *          - a Map of the communication protocol used by each Orb client
   * @param vertexClass
   *          - the type of Vertex to be queued. User-defined by extension of Vertex
   * @param partitionId
   *          - the ID of the partition that creates and owns this OutboundVertexQueue
   */
  public OutboundVertexQueue(int numberOfPartitions,
                             int maxVertices,
                             Map<Integer,OrbPartitionCommunicationProtocol> orbClients,
                             Class<? extends Vertex<?,?,?>> vertexClass,
                             int partitionId) {
    ovqLogger = LoggerFactory.getLogger(OutboundVertexQueue.class);
    
    this.numberOfPartitions = numberOfPartitions;
    this.maxVertices = maxVertices;
    this.orbClients = orbClients;
    this.vertexClass = vertexClass;
    this.partitionId = partitionId;
    
    List<Vertices> partitionVertexList;
    List<Integer> partitionVertexCounter;
    
    // creates an ArrayList of Vertices; each entry in the ArrayList represents a partition
    partitionVertexList = new ArrayList<Vertices>(numberOfPartitions);
    for (int i = 0; i < numberOfPartitions; i++) {
      Vertices vs = new Vertices(vertexClass);
      partitionVertexList.add(vs);
    }
    
    // initializes a vertex counter for each outbound partition
    partitionVertexCounter = new ArrayList<Integer>(numberOfPartitions);
    for (int i = 0; i < numberOfPartitions; i++) {
      partitionVertexCounter.add(0);
    }
    
    this.pvo = new PartitionVertexObject(partitionVertexList, partitionVertexCounter);
  }
  
  /**
   * This method queues up a Vertex to be sent. Once the Vertex count reaches the maximum number, it sends the
   * vertices via Hadoop RPC.
   * 
   * @param v
   *          - a Vertex to be sent
   */
  public void sendVertex(Vertex<?,?,?> v) {
    synchronized (pvo) {
      int vertexHash = Math.abs(v.getVertexID().hashCode()) % numberOfPartitions;
      Vertices currentPartition = pvo.partitionVertexList.get(vertexHash);
      
      Integer vertexCounter;
      synchronized (pvo.partitionVertexCounter) {
        synchronized (currentPartition) {
          vertexCounter = pvo.partitionVertexCounter.get(vertexHash);
          vertexCounter++;
          pvo.partitionVertexCounter.set(vertexHash, vertexCounter);
          
          currentPartition.add(v);
          
          // once the expected number of vertices is met, begins the send operation
          if (vertexCounter >= maxVertices) {
            Vertices verticesToBeSent = currentPartition;
            Vertices vs = new Vertices(vertexClass);
            
            // logger stuff
            ovqLogger.info(this.toString() + " Partition: " + Integer.toString(partitionId)
                           + " Sending bulk vertices. Count: " + vertexCounter + ", "
                           + verticesToBeSent.size());
            
            verticesToBeSent.setVertexType(vertexClass);
            orbClients.get(vertexHash).sendVertices(verticesToBeSent);
            currentPartition = vs;
            pvo.partitionVertexCounter.set(vertexHash, new Integer(0));
          }
          pvo.partitionVertexList.set(vertexHash, currentPartition);
        }
      }
    }
  }
  
  /**
   * Sends any remaining vertices if the maximum number of vertices is not met.
   */
  public void sendRemainingVertices() {
    
    for (int partitionID = 0; partitionID < numberOfPartitions; partitionID++) {
      ovqLogger.info(this.toString() + " Partition: " + Integer.toString(partitionId)
                     + " Sending bulk vertices. Count: " + pvo.partitionVertexCounter.get(partitionID) + ", "
                     + pvo.partitionVertexList.get(partitionID).size());
      if (pvo.partitionVertexList.get(partitionID) != null) {
        Vertices verticesToBeSent = pvo.partitionVertexList.get(partitionID);
        verticesToBeSent.setVertexType(vertexClass);
        orbClients.get(partitionID).sendVertices(pvo.partitionVertexList.get(partitionID));
      }
    }
  }
  
  /**
   * This inner class defines a PartitionVertexObject, which is used strictly to encapsulate
   * partitionVertexList and partitionVertexCounter into one object for synchronization purposes.
   * 
   */
  class PartitionVertexObject {
    
    List<Vertices> partitionVertexList;
    List<Integer> partitionVertexCounter;
    
    /**
     * This constructs a PartitionVertexObject given a partitionVertexList and a partitionVertexCounter.
     * 
     * @param partitionVertexList
     * @param partitionVertexCounter
     */
    public PartitionVertexObject(List<Vertices> partitionVertexList, List<Integer> partitionVertexCounter) {
      this.partitionVertexList = partitionVertexList;
      this.partitionVertexCounter = partitionVertexCounter;
    }
    
    /**
     * Return the verticesList.
     */
    public List<Vertices> getVerticesList() {
      return partitionVertexList;
    }
    
    /**
     * Return the vertexCounter.
     */
    public List<Integer> getPartitionVertexCounter() {
      return partitionVertexCounter;
    }
    
  }
}
