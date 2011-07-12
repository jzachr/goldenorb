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
package org.goldenorb.types.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.goldenorb.types.message.FloatMessage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.ipc.RPC;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SampleFloatMessageTest {
  public final static float MESSAGE_VALUE = 3.14159f;
  public final static String DESTINATION_VALUE = "Destination!";
  public final static int SERVER_PORT = 13000;
  
  private RPCProtocol<FloatMessage,FloatWritable> client;
  private RPCServer<FloatMessage,FloatWritable> server;
  
  private FloatMessage fm0 = new FloatMessage();
  
/**
 * Constructor
 *
 */
  public SampleFloatMessageTest() {
    fm0.setMessageValue(new FloatWritable(MESSAGE_VALUE));
    fm0.setDestinationVertex(DESTINATION_VALUE);
  }
  
/**
 * 
 */
  @SuppressWarnings("unchecked")
  @Before
  public void startServer() throws IOException {
    server = new RPCServer<FloatMessage,FloatWritable>(SERVER_PORT);
    server.start();
    Configuration conf = new Configuration();
    InetSocketAddress addr = new InetSocketAddress("localhost", SERVER_PORT);
    if (client == null) client = (RPCProtocol<FloatMessage,FloatWritable>) RPC.waitForProxy(
      RPCProtocol.class, RPCProtocol.versionID, addr, conf);
  }
  
  @Test
  public void testRPC() {
    FloatMessage fm1 = client.sendAndReceiveMessage(fm0, DESTINATION_VALUE, new FloatWritable(
        MESSAGE_VALUE));
    assertTrue(fm0.get() == client.getMessage().get());
    assertEquals(fm0.getDestinationVertex(), client.getMessage().getDestinationVertex());
    assertEquals(fm1.getDestinationVertex(), DESTINATION_VALUE);
    assertTrue(((FloatWritable) fm1.getMessageValue()).get() == MESSAGE_VALUE);
  }
  
/**
 * 
 */
  @After
  public void stopServer() {
    server.stop();
  }
}
