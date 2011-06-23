package org.goldenorb.queue;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.goldenorb.Message;
import org.goldenorb.Messages;
import org.goldenorb.OrbPartitionCommunicationProtocol;
import org.goldenorb.Vertices;

public class QueueInfoCollector implements OrbPartitionCommunicationProtocol {
  
  List<Message> list;
  
  @Override
  public void sendMessages(Messages messages) {
    //list = messages.getList();
    //System.out.println("I got called.");
    //System.out.println(messages.toString());
  }
  
  public List<Message> getMessageList() {
    return list;
  }
  
  @Override
  public void sendVertices(Vertices vertices) {
    
  }
  
  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return versionID;
  }
  
}
