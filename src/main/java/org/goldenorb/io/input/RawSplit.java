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
package org.goldenorb.io.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class RawSplit implements Writable {
    private String splitClass;
    private BytesWritable bytes = new BytesWritable();
    private String[] locations;
    long dataLength;

/**
 * Set the bytes
 * @param  byte[] data
 * @param  int offset
 * @param  int length
 */
    public void setBytes(byte[] data, int offset, int length) {
      bytes.set(data, offset, length);
    }

/**
 * Set the className
 * @param  String className
 */
    public void setClassName(String className) {
      splitClass = className;
    }
      
/**
 * Return the className
 */
    public String getClassName() {
      return splitClass;
    }
      
/**
 * Return the bytes
 */
    public BytesWritable getBytes() {
      return bytes;
    }

/**
 * 
 */
    public void clearBytes() {
      bytes = null;
    }
      
/**
 * Set the locations
 * @param  String[] locations
 */
    public void setLocations(String[] locations) {
      this.locations = locations;
    }
      
/**
 * Return the locations
 */
    public String[] getLocations() {
      return locations;
    }
      
/**
 * 
 * @param  DataInput in
 */
    public void readFields(DataInput in) throws IOException {
      splitClass = Text.readString(in);
      dataLength = in.readLong();
      bytes.readFields(in);
      int len = WritableUtils.readVInt(in);
      locations = new String[len];
      for(int i=0; i < len; ++i) {
        locations[i] = Text.readString(in);
      }
    }
      
/**
 * 
 * @param  DataOutput out
 */
    public void write(DataOutput out) throws IOException {
      Text.writeString(out, splitClass);
      out.writeLong(dataLength);
      bytes.write(out);
      WritableUtils.writeVInt(out, locations.length);
      for(int i = 0; i < locations.length; i++) {
        Text.writeString(out, locations[i]);
      }        
    }

/**
 * Return the dataLength
 */
    public long getDataLength() {
      return dataLength;
    }
/**
 * Set the dataLength
 * @param  long l
 */
    public void setDataLength(long l) {
      dataLength = l;
    }
  }