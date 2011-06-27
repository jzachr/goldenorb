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

import org.apache.hadoop.io.BooleanWritable;
import org.goldenorb.Message;

public class BooleanMessage extends Message<BooleanWritable> {
  /**
   * Constructor
   * 
   */
  public BooleanMessage() {
    super(BooleanWritable.class);
  }
  
  /**
   * Constructor
   * 
   * @param destinationVertex
   *          - String
   * @param messageValue
   *          - BooleanWritable
   */
  public BooleanMessage(String destinationVertex, BooleanWritable messageValue) {
    super(BooleanWritable.class);
    this.setDestinationVertex(destinationVertex);
    this.setMessageValue(messageValue);
  }
  
  /**
   * Constructor
   * 
   * @param destinationVertex
   *          - String
   * @param value
   *          - boolean
   */
  public BooleanMessage(String destinationVertex, boolean value) {
    super(BooleanWritable.class);
    this.setDestinationVertex(destinationVertex);
    this.setMessageValue(new BooleanWritable(value));
  }
  
  /**
   * Return the primitive boolean value stored in the Writable.
   */
  public boolean get() {
    return ((BooleanWritable) this.getMessageValue()).get();
  }
  
  /**
   * Set the Writable value to the specified boolean.
   * 
   * @param value
   *          - boolean
   */
  public void set(boolean value) {
    ((BooleanWritable) this.getMessageValue()).set(value);
  }
}
