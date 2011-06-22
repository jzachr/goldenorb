package org.goldenorb.queue;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.goldenorb.Message;
import org.junit.Test;

public class InboundMessageQueueTest {
  
  /**
   * Tests mapping 2 messages manually to a single Vertex.
   * 
   * @throws Exception
   */
  @Test
  public void testSingleVertex() throws Exception {
    InboundMessageQueue imqTest = new InboundMessageQueue();
    
    Message<Text> msg1 = new Message<Text>(Text.class);
    msg1.setDestinationVertex("Test Vertex");
    msg1.setMessageValue(new Text("Test Message"));
    imqTest.addMessage(msg1);
    
    Message<Text> msg2 = new Message<Text>(Text.class);
    msg2.setDestinationVertex("Test Vertex");
    msg2.setMessageValue(new Text("testtesttest"));
    imqTest.addMessage(msg2);
    
    List<Message<? extends Writable>> list = imqTest.getMessage("Test Vertex");
    
    assertTrue(list.get(0) == msg1);
    assertTrue(list.get(1) == msg2);
  }
  
  /**
   * Tests mapping messages to many Vertices using threads.
   * 
   * @throws Exception
   */
  @Test
  public void testInboundMessageQueue() throws Exception {
    int numOfThreads = 100;
    InboundMessageQueue imq = new InboundMessageQueue();
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch everyoneDoneLatch = new CountDownLatch(numOfThreads);
    
    // create new MessageThreads that add the passed message to the inbound message queue
    for (int i = 0; i < numOfThreads; i++) {
      Message<Text> msg = new Message<Text>(Text.class);
      msg.setDestinationVertex(Integer.toString(i));
      msg.setMessageValue(new Text("test message " + Integer.toString(i)));
      MessageThread mThread = new MessageThread(msg, imq, startLatch, everyoneDoneLatch);
      mThread.start();
    }
    
    startLatch.countDown(); // start the threads simultaneously
    
    everyoneDoneLatch.await(); // wait until all threads are done
    
    Iterator<String> iter = imq.getVerticesWithMessages().iterator();
    int count = 0;
    while (iter.hasNext()) {
      iter.next();
      count++;
    }
    
    assertTrue(count == numOfThreads);
    assertThat(InboundMessageQueue.class, notNullValue());
  }
}

/**
 * This class defines the Threads that can be used to add messages to an InboundMessageQueue simultaneously.
 * 
 * @author long
 * 
 */
class MessageThread extends Thread {
  
  private Message<? extends Writable> msg;
  private InboundMessageQueue imq;
  private CountDownLatch startLatch;
  private CountDownLatch everyoneDoneLatch;
  
  /**
   * Constructs a MessageThread.
   * 
   * @param msg
   * @param imq
   * @param startLatch
   * @param everyoneDoneLatch
   */
  public MessageThread(Message<? extends Writable> msg,
                       InboundMessageQueue imq,
                       CountDownLatch startLatch,
                       CountDownLatch everyoneDoneLatch) {
    this.msg = msg;
    this.imq = imq;
    this.startLatch = startLatch;
    this.everyoneDoneLatch = everyoneDoneLatch;
  }
  
  /**
   * Adds a message to the InboundMessageQueue.
   */
  public void run() {
    try {
      startLatch.await();
      imq.addMessage(msg);
      everyoneDoneLatch.countDown();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (NullPointerException e) {
      e.printStackTrace();
    }
  }
}
