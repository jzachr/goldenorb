package org.goldenorb.algorithms.singleSourceShortestPath;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.goldenorb.Message;
import org.goldenorb.types.ArrayListWritable;

public class PathMessage extends Message<PathWritable> {
  
  /**
   * Licensed to Ravel, Inc. under one or more contributor license agreements. See the NOTICE file distributed
   * with this work for additional information regarding copyright ownership. Ravel, Inc. licenses this file
   * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in
   * compliance with the License. You may obtain a copy of the License at
   * 
   * http://www.apache.org/licenses/LICENSE-2.0
   * 
   * Unless required by applicable law or agreed to in writing, software distributed under the License is
   * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   * See the License for the specific language governing permissions and limitations under the License.
   * 
   */
  
  /**
   * Constructor
   * 
   */
  public PathMessage() {
    super(PathWritable.class);
  }
  
  /**
   * Constructor
   * 
   * @param destinationVertex
   *          - String
   * @param messageValue
   *          - IntWritable
   */
  public PathMessage(String destinationVertex, PathWritable messageValue) {
    super(PathWritable.class);
    this.setDestinationVertex(destinationVertex);
    this.setMessageValue(messageValue);
  }
  
  /**
   * Return the primitive int value stored in the Writable.
   */
  public int getWeight() {
    return ((PathWritable) this.getMessageValue()).getWeight().get();
  }
  
  /**
   * Set the Writable value to the specified int.
   * 
   * @param value
   *          - boolean
   */
  public void setWeight(int value) {
    ((PathWritable) this.getMessageValue()).setWeight(new IntWritable(value));
  }
  
  /**
   * Add a vertex.
   */
  @SuppressWarnings("unchecked")
  public ArrayListWritable<Text> getVertices() {
    return ((PathWritable) this.getMessageValue()).getVertices();
  }
  
  /**
   * Set the Writable value to the specified int.
   * 
   * @param value
   *          - boolean
   */
  public void setVertices(ArrayListWritable<Text> value) {
    ((PathWritable) this.getMessageValue()).setVertices(value);
  }
}
