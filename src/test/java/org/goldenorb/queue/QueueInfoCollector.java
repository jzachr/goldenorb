package org.goldenorb.queue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.goldenorb.Message;
import org.goldenorb.Messages;
import org.goldenorb.OrbPartitionCommunicationProtocol;
import org.goldenorb.Vertex;
import org.goldenorb.Vertices;
import org.goldenorb.io.input.RawSplit;

public class QueueInfoCollector implements OrbPartitionCommunicationProtocol {
  
  List<Message> mList = Collections.synchronizedList(new ArrayList<Message>());
  List<Vertex> vList = Collections.synchronizedList(new ArrayList<Vertex>());
  
  @Override
  public void sendMessages(Messages messages) {
    // add all outgoing Messages to a synchronizedList to check if any Messages are lost
    mList.addAll(messages.getList());
  }
  
  @Override
  public void sendVertices(Vertices vertices) {
 // add all outgoing Vertices to a synchronizedList to check if any messages are lost
    vList.addAll(vertices.getArrayList());
  }
  
  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return versionID;
  }

  @Override
  public void becomeActive() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void loadVerticesFromInputSplit(RawSplit rawsplit) {
    // TODO Auto-generated method stub
    
  }
  
}
