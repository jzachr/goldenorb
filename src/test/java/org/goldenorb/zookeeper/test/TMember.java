package org.goldenorb.zookeeper.test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.goldenorb.zookeeper.Member;

public class TMember implements Member {
  
  private int data;
  
  public TMember() {}
  
  public void setData(int data) {
    this.data = data;
  }
  
  public int getData() {
    return data;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
     data = in.readInt();
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(data);
  }
  
  @Override
  public String toString(){
    return Integer.toString(data);
  }
  
  @Override
  public boolean equals(Object rhs){
    return this.getData() == ((TMember)rhs).getData();
  }
}
