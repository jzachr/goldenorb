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

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.io.IntWritable;
import org.goldenorb.Edge;
import org.goldenorb.OrbPartitionCommunicationProtocol;
import org.goldenorb.Vertex;
import org.goldenorb.Vertices;
import org.junit.Test;

/**
 * Tests the OutboundVertexQueue by using multithreading and sending large numbers of vertices.
 */
public class OutboundVertexQueueTest {
  
  QueueInfoCollector infoCollector = new QueueInfoCollector();
  
  @Test
  public void testOutBoundVertexQueue() throws Exception {
    // OutboundVertexQueue settings
    int numberOfPartitions = 100;
    int numOfVerticesToSendPerThread = 10500; // max number of Vertices to be sent by a Thread
    int numOfVerticesPerBlock = 1000; // max number of Vertices to trigger a send operation by the queue
    int partitionId = 1;
    Class<? extends Vertex<?,?,?>> vertexClass = TestVertex.class;
    Map<Integer,OrbPartitionCommunicationProtocol> orbClients = new HashMap<Integer,OrbPartitionCommunicationProtocol>();
    for (int i = 0; i < numberOfPartitions; i++) {
      orbClients.put(new Integer(i), infoCollector);
    }
    
    OutboundVertexQueue ovq = new OutboundVertexQueue(numberOfPartitions, numOfVerticesPerBlock, orbClients,
        vertexClass, partitionId);
    
    // initialize the Threads and pass them their test Vertices
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch everyoneDoneLatch = new CountDownLatch(numberOfPartitions);
    for (int i = 0; i < numberOfPartitions; i++) {
      Vertices vrts = new Vertices(vertexClass);
      for (int p = 0; p < numOfVerticesToSendPerThread; p++) {
        String vertexID = "vertex " + p;
        IntWritable vertexValue = new IntWritable(p);
        List<Edge<IntWritable>> edgesList = new ArrayList<Edge<IntWritable>>();
        TestVertex vrt = new TestVertex(vertexID, vertexValue, edgesList);
        vrts.add(vrt);
      }
      
      OutboundVertexThread obmThread = new OutboundVertexThread(vrts, ovq, startLatch, everyoneDoneLatch);
      obmThread.start(); // initialize a Thread
    }
    
    startLatch.countDown(); // start all Threads simultaneously
    
    everyoneDoneLatch.await(); // wait until all Threads are done
    
    ovq.sendRemainingVertices();
    
    System.out.println(infoCollector.vList.size());
    
    assertThat(ovq, notNullValue());
    assertTrue(infoCollector.vList.size() == (numberOfPartitions * numOfVerticesToSendPerThread));
  }
}

/**
 * This class defines the Threads that can be used to add vertices to an OutboundVertexQueue simultaneously.
 */
class OutboundVertexThread extends Thread {
  
  private Vertices vrts;
  private OutboundVertexQueue ovq;
  private CountDownLatch startLatch;
  private CountDownLatch everyoneDoneLatch;
  
  /**
   * Constructs an OutboundVertexThread.
   * 
   * @param msgs
   * @param ovq
   * @param startLatch
   * @param everyoneDoneLatch
   */
  public OutboundVertexThread(Vertices vrts,
                              OutboundVertexQueue ovq,
                              CountDownLatch startLatch,
                              CountDownLatch everyoneDoneLatch) {
    this.vrts = vrts;
    this.ovq = ovq;
    this.startLatch = startLatch;
    this.everyoneDoneLatch = everyoneDoneLatch;
  }
  
  /**
   * Adds vertices to the OutboundVertexQueue.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void run() {
    try {
      startLatch.await();
      for (Vertex vrt : vrts.getArrayList()) {
        ovq.sendVertex(vrt);
      }
      everyoneDoneLatch.countDown();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (NullPointerException e) {
      e.printStackTrace();
    }
  }
}