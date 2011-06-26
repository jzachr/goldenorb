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
package org.goldenorb.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * This class defines a StreamWriter, a utility class which writes from an InputStream to an OutputStream.
 * 
 */
public class StreamWriter extends Thread {
  
  OutputStream os;
  InputStream is;
  
  /**
   * Constructor
   * 
   * @param is
   *          - InputStream
   * @param os
   *          - OutputStream
   */
  public StreamWriter(InputStream is, OutputStream os) {
    this.is = is;
    this.os = os;
    start();
  }
  
  /**
   * Runs the StreamWriter.
   */
  public void run() {
    byte b[] = new byte[80];
    int rc;
    try {
      while ((rc = is.read(b)) > 0) {
        os.write(b, 0, rc);
      }
    } catch (IOException e) {}
    
  }
}
