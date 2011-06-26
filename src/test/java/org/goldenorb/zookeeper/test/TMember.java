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
package org.goldenorb.zookeeper.test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.zookeeper.Member;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;

public class TMember implements Member {
  
  private int data;
  
/**
 * Constructor
 *
 */
  public TMember() {}
  
/**
 * Set the data
 * @param  int data
 */
  public void setData(int data) {
    this.data = data;
  }
  
/**
 * Return the data
 */
  public int getData() {
    return data;
  }
  
/**
 * 
 * @param  int data
 * @param  ZooKeeper zk
 * @param  String path
 */
  public void changeData(int data, ZooKeeper zk, String path) throws OrbZKFailure {
    this.data = data;
    ZookeeperUtils.setNodeData(zk, path, this);
  }
  
/**
 * 
 * @param  DataInput in
 */
  @Override
  public void readFields(DataInput in) throws IOException {
     data = in.readInt();
  }
  
/**
 * 
 * @param  DataOutput out
 */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(data);
  }
  
/**
 * 
 * @returns String
 */
  @Override
  public String toString(){
    return Integer.toString(data);
  }
  
/**
 * 
 * @param  Object rhs
 * @returns boolean
 */
  @Override
  public boolean equals(Object rhs){
    return this.getData() == ((TMember)rhs).getData();
  }
}
