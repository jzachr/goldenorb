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
 * 
 */
package org.goldenorb.queue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.goldenorb.Message;
import org.goldenorb.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides a Queue construct that collects inbound messages for a partition. After a certain
 * number of messages is collected, it sends out the messages to the appropriate destination vertices.
 * 
 */
public class InboundMessageQueue {
  
  private Map<String,List<Message<? extends Writable>>> inboundMessageMap = new HashMap<String,List<Message<? extends Writable>>>();
  private Set<String> verticesWithMessages = new HashSet<String>();
  private Logger logger;
  
  /**
   * Constructs a InboundMessageQueue.
   * 
   */
  public InboundMessageQueue() {
    logger = LoggerFactory.getLogger(InboundMessageQueue.class);
  }
  
  /**
   * Adds the Message objects contained in the given Messages object.
   * 
   * @param ms
   *          - a Messages object containing multiple Message objects
   */
  public void addMessages(Messages ms) {
    for (Message<? extends Writable> m : ms.getList()) {
      addMessage(m);
    }
  }
  
  /**
   * Adds a Message to the InboundMessageQueue.
   * 
   * @param m
   *          - a Message to be added
   */
  public void addMessage(Message<? extends Writable> m) {
    synchronized (inboundMessageMap) {
      // creates an empty, synchronized ArrayList for a key (a Vertex) if the key doesn't already exist
      if (inboundMessageMap.containsKey(m.getDestinationVertex()) == false) {
        inboundMessageMap.put(m.getDestinationVertex(),
          Collections.synchronizedList(new ArrayList<Message<? extends Writable>>()));
      }
      
      inboundMessageMap.get(m.getDestinationVertex()).add(m);
      synchronized (verticesWithMessages) {
        verticesWithMessages.add(m.getDestinationVertex());
      }
    }
  }
  
  /**
   * Adds a new Vertex to the internal inboundMessageMap.
   * 
   * @param id
   *          - String name of the Vertex
   */
  public void addNewVertex(String id) {
    synchronized (inboundMessageMap) {
      inboundMessageMap.put(id, Collections.synchronizedList(new ArrayList<Message<? extends Writable>>()));
    }
  }
  
  /**
   * Return the List of messages currently in the queue for a specificed Vertex.
   * 
   * @param vertexID
   */
  public List<Message<? extends Writable>> getMessage(String vertexID) {
    return inboundMessageMap.get(vertexID);
  }
  
  /**
   * Return the inboundMessageMap.
   */
  public Map<String,List<Message<? extends Writable>>> getInboundMessageMap() {
    return inboundMessageMap;
  }
  
  /**
   * Return the verticesWithMessages.
   */
  public Set<String> getVerticesWithMessages() {
    return verticesWithMessages;
  }
}
