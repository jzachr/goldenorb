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
  
  public Message() {}
  
  public Message(Class<MV> messageValueClass) {
    this.messageValueClass = messageValueClass;
  }
  
  public String getDestinationVertex() {
    return destinationVertex;
  }
  
  public void setDestinationVertex(String destinationVertex) {
    this.destinationVertex = destinationVertex;
  }
  
  public Writable getMessageValue() {
    return messageValue;
  }
  
  public void setMessageValue(MV messageValue) {
    this.messageValue = messageValue;
  }
  
  public Class<? extends Writable> getMessageValueClass() {
    return messageValueClass;
  }
  
  @Override
  public String toString() {
    return "destination: \"" + destinationVertex + "\", value:{ " + messageValue + " }";
  }
  
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
  
  public void write(DataOutput out) throws IOException {
    out.writeUTF(destinationVertex);
    messageValue.write(out);
  }
}
