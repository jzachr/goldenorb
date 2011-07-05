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
package org.goldenorb.util.message;

import org.apache.hadoop.io.IntWritable;
import org.goldenorb.Message;
import org.goldenorb.Vertex;

public class IntSourceMessage extends Message<IntWritable> {
  
	private String sourceVertex;

	public String getSourceVertex() {
		return sourceVertex;
	}

	public void setSourceVertex(String sourceVertex) {
		this.sourceVertex = sourceVertex;
	}

	/**
   * Constructor
   * 
   */
  public IntSourceMessage(Vertex v) {
    super(IntWritable.class);
    this.setSourceVertex(v.getVertexID());
  }
  
  /**
   * Constructor
   * 
   * @param destinationVertex
   *          - String
   * @param messageValue
   *          - IntWritable
   */
  public IntSourceMessage(Vertex v, String destinationVertex, IntWritable messageValue) {
    super(IntWritable.class);
    this.setDestinationVertex(destinationVertex);
    this.setMessageValue(messageValue);
    this.setSourceVertex(v.getVertexID());
  }
  
  /**
   * Constructor
   * 
   * @param destinationVertex
   *          - String
   * @param value
   *          - int
   */
  public IntSourceMessage(Vertex v, String destinationVertex, int value) {
    super(IntWritable.class);
    this.setDestinationVertex(destinationVertex);
    this.setMessageValue(new IntWritable(value));
    this.setSourceVertex(v.getVertexID());
  }
  
  /**
   * Return the primitive int value stored in the Writable.
   */
  public int get() {
    return ((IntWritable) this.getMessageValue()).get();
  }
  
  /**
   * Set the Writable value to the specified int.
   * 
   * @param value
   *          - boolean
   */
  public void set(int value) {
    ((IntWritable) this.getMessageValue()).set(value);
  }
}
