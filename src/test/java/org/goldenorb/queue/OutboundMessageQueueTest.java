package org.goldenorb.queue;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.goldenorb.Message;
import org.goldenorb.Messages;
import org.goldenorb.OrbPartitionCommunicationProtocol;
import org.goldenorb.types.message.TextMessage;
import org.junit.Test;

public class OutboundMessageQueueTest {
  
  QueueInfoCollector infoCollector = new QueueInfoCollector();
  
  @Test
  public void testOutBoundMessageQueue() throws Exception {
    // OutboundMessageQueue settings
    int numberOfPartitions = 100;
    int numOfMessagesToSendPerThread = 10000; // max number of Messages to be sent by a Thread
    int numOfMessagesPerBlock = 1000; // max number of Messages to trigger a send operation by the queue
    int partitionId = 1;
    Class<? extends Message<? extends Writable>> messageClass = TextMessage.class;
    Map<Integer,OrbPartitionCommunicationProtocol> orbClients = new HashMap<Integer,OrbPartitionCommunicationProtocol>();
    for (int i = 0; i < numberOfPartitions; i++) {
      orbClients.put(new Integer(i), infoCollector);
    }
    
    OutboundMessageQueue omq = new OutboundMessageQueue(numberOfPartitions, numOfMessagesPerBlock,
        orbClients, messageClass, partitionId);
    
    // initialize the Threads and pass them their test Messages
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch everyoneDoneLatch = new CountDownLatch(numberOfPartitions);
    for (int i = 0; i < numberOfPartitions; i++) {
      Messages msgs = new Messages(TextMessage.class);
      for (int p = 0; p < numOfMessagesToSendPerThread; p++) {
        TextMessage txtmsg = new TextMessage(Integer.toString(i), new Text("test message "
                                                                           + Integer.toString(p)));
        msgs.add(txtmsg);
      }
      
      OutboundMessageThread obmThread = new OutboundMessageThread(msgs, omq, startLatch, everyoneDoneLatch);
      obmThread.start(); // initialize a Thread
    }
    
    startLatch.countDown(); // start all Threads simultaneously
    
    everyoneDoneLatch.await(); // wait until all Threads are done
    
    assertThat(omq, notNullValue());
    assertTrue(infoCollector.mList.size() == (numberOfPartitions * numOfMessagesToSendPerThread));
  }
}

/**
 * This class defines the Threads that can be used to add messages to an OutboundMessageQueue simultaneously.
 * 
 * @author long
 * 
 */
class OutboundMessageThread extends Thread {
  
  private Messages msgs;
  private OutboundMessageQueue omq;
  private CountDownLatch startLatch;
  private CountDownLatch everyoneDoneLatch;
  
  /**
   * Constructs an OutboundMessageThread.
   * 
   * @param msgs
   * @param omq
   * @param startLatch
   * @param everyoneDoneLatch
   */
  public OutboundMessageThread(Messages msgs,
                               OutboundMessageQueue omq,
                               CountDownLatch startLatch,
                               CountDownLatch everyoneDoneLatch) {
    this.msgs = msgs;
    this.omq = omq;
    this.startLatch = startLatch;
    this.everyoneDoneLatch = everyoneDoneLatch;
  }
  
  /**
   * Adds messages to the OutboundMessageQueue.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void run() {
    try {
      startLatch.await();
      for (Message msg : msgs.getList()) {
        omq.sendMessage(msg);
      }
      everyoneDoneLatch.countDown();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (NullPointerException e) {
      e.printStackTrace();
    }
  }
}