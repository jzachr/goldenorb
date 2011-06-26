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

public class InboundMessageQueue {
  
  Map<String,List<Message<? extends Writable>>> inboundMessageMap = new HashMap<String,List<Message<? extends Writable>>>();
  Set<String> verticesWithMessages = new HashSet<String>();
  private Logger logger;
  
  /**
   * Constructs the InboundMessageQueue.
   * 
   * @param ids
   */
  public InboundMessageQueue() {
    logger = LoggerFactory.getLogger(InboundMessageQueue.class);
  }
  
/**
 * 
 * @param  Messages ms
 */
  public void addMessages(Messages ms) {
    for (Message<? extends Writable> m : ms.getList()) {
      addMessage(m);
    }
  }
  
/**
 * 
 * @param  Message<? extends Writable> m
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
 * 
 * @param  String id
 */
  public void addNewVertex(String id) {
    synchronized (inboundMessageMap) {
      inboundMessageMap.put(id, Collections.synchronizedList(new ArrayList<Message<? extends Writable>>()));
    }
  }
  
/**
 * Return the message
 */
  public List<Message<? extends Writable>> getMessage(String vertexID) {
    return inboundMessageMap.get(vertexID);
  }
  
/**
 * Return the inboundMessageMap
 */
  public Map<String,List<Message<? extends Writable>>> getInboundMessageMap() {
    return inboundMessageMap;
  }
  
/**
 * Return the verticesWithMessages
 */
  public Set<String> getVerticesWithMessages() {
    return verticesWithMessages;
  }
}
