package org.goldenorb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

@SuppressWarnings("rawtypes")
public class Messages implements Writable {
  
  List<Message> messages;
  
  Class<? extends Message> messageClassType;
  
  public Messages() {
    this.messages = new ArrayList<Message>();
  }
  
  public Messages(Class<? extends Message> messageClassType) {
    this.messages = new ArrayList<Message>();
    this.messageClassType = messageClassType;
  }
  
  public Messages(int size) {
    this.messages = new ArrayList<Message>(size);
  }
  
  public Messages(Class<? extends Message> messageClassType, int size) {
    this.messageClassType = messageClassType;
    this.messages = new ArrayList<Message>(size);
  }
  
  public void add(Message message) {
    messages.add(message);
  }
  
  public int size() {
    return messages.size();
  }
  
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
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(messageClassType.getName());
    out.writeInt(messages.size());
    for (Message message : messages) {
      message.write(out);
    }
  }
  
  public List<Message> getList() {
    return messages;
  }
  
  public void setList(List<Message> messages) {
    this.messages = messages;
  }
}
