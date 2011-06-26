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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.goldenorb.Message;

public class RPCServer<M extends Message<W>, W extends Writable> implements RPCProtocol<M,W> {
  
  private int port;
  private Server server = null;
  
  private M message;
  
/**
 * Constructor
 *
 * @param  int port
 */
  public RPCServer(int port) {
    this.port = port;
    Configuration conf = new Configuration();
    try {
      server = RPC.getServer(this, "localhost", this.port, conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
/**
 * 
 */
  @Override
  public void start() {
    try {
      server.start();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
    
/**
 * 
 * @param  M msg
 * @param  String dst
 * @param  W wrt
 * @returns M
 */
  @SuppressWarnings("unchecked")
  @Override
  public M sendAndReceiveMessage(M msg, String dst, W wrt) {
    M retVal = null;
    message = (M) msg;
    try {
      retVal = ((Class<M>) message.getClass()).newInstance();
      retVal.setDestinationVertex(dst);
      retVal.setMessageValue(wrt);
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
    return retVal;
  }
  
/**
 * Return the message
 */
  public M getMessage() {
    return message;
  }
  
/**
 * Return the protocolVersion
 */
  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return versionID;
  }

/**
 * 
 */
  @Override
  public void stop() {
    server.stop();
  }
  
}
