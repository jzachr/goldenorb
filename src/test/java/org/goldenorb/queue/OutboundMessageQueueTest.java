package org.goldenorb.queue;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.goldenorb.Message;
import org.goldenorb.OrbPartitionCommunicationProtocol;
import org.goldenorb.types.message.TextMessage;
import org.junit.Test;

public class OutboundMessageQueueTest {
  
  QueueInfoCollector infoCollector = new QueueInfoCollector();
  
  @Test
  public void testOutBoundMessageQueue() throws Exception {
    
    // OutboundMessageQueue settings
    int numberOfPartitions = 5;
    int maxMessages = 5000;
    int partitionId = 1;
    Class<? extends Message<? extends Writable>> messageType = TextMessage.class;
    Map<Integer,OrbPartitionCommunicationProtocol> orbClients = new HashMap<Integer,OrbPartitionCommunicationProtocol>();
    // make the orbClients thing
    for (int i = 0; i < numberOfPartitions; i++) {
      orbClients.put(new Integer(i), infoCollector);
    }
    
    OutboundMessageQueue omq = new OutboundMessageQueue(numberOfPartitions, maxMessages, orbClients,
        messageType, partitionId);
    
    int destVertex = 0;
    TextMessage m = new TextMessage(Integer.toString(destVertex), new Text("TEST MESSAGE LOL"));
    
    omq.sendMessage(m);
    
    /*Iterator iter = omq.orbClients.keySet().iterator();
    while (iter.hasNext()) {
      System.out.println(iter.next());
    }
    
    Iterator iter1 = omq.orbClients.entrySet().iterator();
    while (iter1.hasNext()) {
      System.out.println(iter1.next());
    } */
    
    assertThat(omq, notNullValue());
  }
}
