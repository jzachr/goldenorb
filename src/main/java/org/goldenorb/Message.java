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
package org.goldenorb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * A Writable for the messages that will be sent between vertices. You must subclass this class, and include
 * the the type in the constructor of the subclass if you wish to be able to use it in hadoop rpc.
 * 
 * For example: <code>
 * public class IntMessage extends Message<IntWritable/> {
 *   public IntMessage() { 
 *     super(IntWritable.class); 
 *   }	
 * }
 * </code>
 */
public class Message<MV extends Writable> implements Writable {
  
  private String destinationVertex;
  private MV messageValue;
  Class<MV> messageValueClass;
  
/**
 * Constructor
 *
 */
  public Message() {}
  
/**
 * Constructor
 *
 * @param  Class<MV> messageValueClass
 */
  public Message(Class<MV> messageValueClass) {
    this.messageValueClass = messageValueClass;
  }
  
/**
 * Return the destinationVertex
 */
  public String getDestinationVertex() {
    return destinationVertex;
  }
  
/**
 * Set the destinationVertex
 * @param  String destinationVertex
 */
  public void setDestinationVertex(String destinationVertex) {
    this.destinationVertex = destinationVertex;
  }
  
/**
 * Return the messageValue
 */
  public Writable getMessageValue() {
    return messageValue;
  }
  
/**
 * Set the messageValue
 * @param messageValue
 */
  public void setMessageValue(MV messageValue) {
    this.messageValue = messageValue;
  }
  
/**
 * Return the messageValueClass
 */
  public Class<? extends Writable> getMessageValueClass() {
    return messageValueClass;
  }
  
/**
 * 
 * @returns String
 */
  @Override
  public String toString() {
    return "destination: \"" + destinationVertex + "\", value:{ " + messageValue + " }";
  }
  
/**
 * Deserialize the fields of this object from in.
 * @param  DataInput in
 */
  public void readFields(DataInput in) throws IOException {
    destinationVertex = in.readUTF();
    try {
      messageValue = messageValueClass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    messageValue.readFields(in);
  }
  
/**
 * 
 * @param  DataOutput out
 */
  public void write(DataOutput out) throws IOException {
    out.writeUTF(destinationVertex);
    messageValue.write(out);
  }
}
