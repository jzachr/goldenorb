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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * A Writable container to store messages.
 */
@SuppressWarnings("rawtypes")
public class Messages implements Writable {
  
  List<Message> messages;
  
  Class<? extends Message> messageClassType;
  
/**
 * Constructor
 *
 */
  public Messages() {
    this.messages = new ArrayList<Message>();
  }
  
/**
 * Constructor
 *
 * @param  Class<? extends Message> messageClassType
 */
  public Messages(Class<? extends Message> messageClassType) {
    this.messages = new ArrayList<Message>();
    this.messageClassType = messageClassType;
  }
  
/**
 * Constructor
 *
 * @param  int size
 */
  public Messages(int size) {
    this.messages = new ArrayList<Message>(size);
  }
  
/**
 * Constructor
 *
 * @param  Class<? extends Message> messageClassType
 * @param  int size
 */
  public Messages(Class<? extends Message> messageClassType, int size) {
    this.messageClassType = messageClassType;
    this.messages = new ArrayList<Message>(size);
  }
  
/**
 * 
 * @param  message - Message to add
 */
  public void add(Message message) {
    messages.add(message);
  }
  
/**
 * 
 * @returns int - the number of elements
 */
  public int size() {
    return messages.size();
  }
  
/**
 * 
 * @param  DataInput in
 */
  @SuppressWarnings("unchecked")
  @Override
  public void readFields(DataInput in) throws IOException {
    try {
      String className = in.readUTF();
      messageClassType = (Class<? extends Message>) Class.forName(className);
      int arrayListSize = in.readInt();
      messages = new ArrayList<Message>(arrayListSize);
      for (int i = 0; i < arrayListSize; i++) {
        Message messageIn = null;
        try {
          messageIn = messageClassType.newInstance();
        } catch (InstantiationException e) {
          throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
        messageIn.readFields(in);
        messages.add(messageIn);
      }
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
  
/**
 * 
 * @param  DataOutput out
 */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(messageClassType.getName());
    out.writeInt(messages.size());
    for (Message message : messages) {
      message.write(out);
    }
  }
  
/**
 * Return the list
 */
  public List<Message> getList() {
    return messages;
  }
  
/**
 * Set the list
 * @param  List<Message> messages
 */
  public void setList(List<Message> messages) {
    this.messages = messages;
  }
}
