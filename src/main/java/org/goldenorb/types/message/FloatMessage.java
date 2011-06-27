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
package org.goldenorb.types.message;

import org.apache.hadoop.io.FloatWritable;
import org.goldenorb.Message;

public class FloatMessage extends Message<FloatWritable> {
  /**
   * Constructor
   * 
   */
  public FloatMessage() {
    super(FloatWritable.class);
  }
  
  /**
   * Constructor
   * 
   * @param destinationVertex
   *          - String
   * @param messageValue
   *          - FloatWritable
   */
  public FloatMessage(String destinationVertex, FloatWritable messageValue) {
    super(FloatWritable.class);
    this.setDestinationVertex(destinationVertex);
    this.setMessageValue(messageValue);
  }
  
  /**
   * Constructor
   * 
   * @param destinationVertex
   *          - String
   * @param value
   *          - float
   */
  public FloatMessage(String destinationVertex, float value) {
    super(FloatWritable.class);
    this.setDestinationVertex(destinationVertex);
    this.setMessageValue(new FloatWritable(value));
  }
  
  /**
   * Return the primitive float value stored in the Writable.
   */
  public float get() {
    return ((FloatWritable) this.getMessageValue()).get();
  }
  
  /**
   * Set the Writable value to the specified float.
   * 
   * @param value
   *          - boolean
   */
  public void set(float value) {
    ((FloatWritable) this.getMessageValue()).set(value);
  }
}
