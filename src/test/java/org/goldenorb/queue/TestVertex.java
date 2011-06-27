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

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.goldenorb.Edge;
import org.goldenorb.Vertex;
import org.goldenorb.types.message.TextMessage;

public class TestVertex extends Vertex<IntWritable,IntWritable,TextMessage> {
  
  /**
   * Constructor
   * 
   */
  public TestVertex() {
    super(IntWritable.class, IntWritable.class, TextMessage.class);
  }
  
  /**
   * Constructor
   * 
   * @param vertexID
   * @param value
   * @param edges
   */
  public TestVertex(String vertexID, IntWritable value, List<Edge<IntWritable>> edges) {
    super(vertexID, value, edges);
  }
  
  /**
   * Empty compute method.
   * 
   * @param messages
   */
  @Override
  public void compute(Collection<TextMessage> messages) {
    this.voteToHalt();
  }
  
}
