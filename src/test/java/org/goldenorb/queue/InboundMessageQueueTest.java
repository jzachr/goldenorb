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
import org.goldenorb.Messages;
import org.goldenorb.types.message.TextMessage;
import org.junit.Test;

/**
 * Tests the InboundMessageQueue by using multithreading and sending large numbers of messages.
 */
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
   * Tests mapping many messages to many Vertices using threads.
   * 
   * @throws Exception
   */
  @Test
  public void testInboundMessageQueue() throws Exception {
    int numOfThreads = 100;
    int numOfMessages = 10000;
    InboundMessageQueue imq = new InboundMessageQueue();
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch everyoneDoneLatch = new CountDownLatch(numOfThreads);
    
    // create new MessageThreads that add the passed message to the inbound message queue
    for (int i = 0; i < numOfThreads; i++) {
      Messages msgs = new Messages(TextMessage.class);
      for (int p = 0; p < numOfMessages; p++) {
        TextMessage txtmsg = new TextMessage(Integer.toString(i), new Text("test message "
                                                                           + Integer.toString(p)));
        msgs.add(txtmsg);
      }
      
      MessageThread mThread = new MessageThread(msgs, imq, startLatch, everyoneDoneLatch);
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
    
    int randomVertex = (int) (Math.random() * (numOfThreads)); // check a random Vertex
    Iterator<Message<? extends Writable>> iter2 = imq.getMessage(Integer.toString(randomVertex)).iterator();
    int count2 = 0;
    while (iter2.hasNext()) {
      iter2.next();
      count2++;
    }
    
    assertTrue(count == numOfThreads);
    assertTrue(count2 == numOfMessages);
    assertThat(imq.getMessage(Integer.toString(randomVertex)), notNullValue());
  }
}

/**
 * This class defines the Threads that can be used to add messages to an InboundMessageQueue simultaneously.
 * 
 */
class MessageThread extends Thread {
  
  private Messages msgs;
  private InboundMessageQueue imq;
  private CountDownLatch startLatch;
  private CountDownLatch everyoneDoneLatch;
  
  /**
   * Constructs a MessageThread.
   * 
   * @param msgs
   * @param imq
   * @param startLatch
   * @param everyoneDoneLatch
   */
  public MessageThread(Messages msgs,
                       InboundMessageQueue imq,
                       CountDownLatch startLatch,
                       CountDownLatch everyoneDoneLatch) {
    this.msgs = msgs;
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
      imq.addMessages(msgs);
      everyoneDoneLatch.countDown();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (NullPointerException e) {
      e.printStackTrace();
    }
  }
}
