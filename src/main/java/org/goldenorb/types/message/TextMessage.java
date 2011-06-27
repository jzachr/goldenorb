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

import org.apache.hadoop.io.Text;
import org.goldenorb.Message;

public class TextMessage extends Message<Text> {
  /**
   * Constructor
   * 
   */
  public TextMessage() {
    super(Text.class);
  }
  
  /**
   * Constructor
   * 
   * @param destinationVertex
   *          - String
   * @param messageValue
   *          - Text
   */
  public TextMessage(String destinationVertex, Text messageValue) {
    super(Text.class);
    this.setDestinationVertex(destinationVertex);
    this.setMessageValue(messageValue);
  }
  
  /**
   * Constructor
   * 
   * @param destinationVertex
   *          - String
   * @param value
   *          - String
   */
  public TextMessage(String destinationVertex, String value) {
    super(Text.class);
    this.setDestinationVertex(destinationVertex);
    this.setMessageValue(new Text(value));
  }
  
  /**
   * Return the primitive boolean value stored in the Writable.
   */
  public String get() {
    return ((Text) this.getMessageValue()).toString();
  }
  
  /**
   * Set the Writable value to the specified boolean.
   * 
   * @param value
   *          - boolean
   */
  public void set(String value) {
    ((Text) this.getMessageValue()).set(value);
  }
}
