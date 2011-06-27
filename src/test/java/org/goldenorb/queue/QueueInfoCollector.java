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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.goldenorb.Message;
import org.goldenorb.Messages;
import org.goldenorb.OrbPartitionCommunicationProtocol;
import org.goldenorb.Vertex;
import org.goldenorb.Vertices;
import org.goldenorb.io.input.RawSplit;

/**
 * This class is a test implementation of OrbPartitionCommunicationProtocol used to collect outbound messages
 * or vertices.
 * 
 */
public class QueueInfoCollector implements OrbPartitionCommunicationProtocol {
  
  List<Message> mList = Collections.synchronizedList(new ArrayList<Message>());
  List<Vertex> vList = Collections.synchronizedList(new ArrayList<Vertex>());
  
  /**
   * When sendMessages is called, the QueueInfoCollector stores all the messages in a List.
   * 
   * @param messages
   */
  @Override
  public void sendMessages(Messages messages) {
    // add all outgoing Messages to a synchronizedList to check if any Messages are lost
    mList.addAll(messages.getList());
  }
  
  /**
   * When sendVertices is called, the QueueInfoCollector stores all the vertices in a List.
   * 
   * @param vertices
   */
  @Override
  public void sendVertices(Vertices vertices) {
    // add all outgoing Vertices to a synchronizedList to check if any messages are lost
    vList.addAll(vertices.getArrayList());
  }
  
  /**
   * Return the protocolVersion.
   */
  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return versionID;
  }
  
  /**
   * Unimplemented in this test class.
   * 
   * @param partitionID
   */
  @Override
  public void becomeActive(int partitionID) {

  }
  
  /**
   * Unimplemented in this test class.
   * 
   * @param rawsplit
   */
  @Override
  public void loadVerticesFromInputSplit(RawSplit rawsplit) {

  }
  
}
