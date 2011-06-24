package org.goldenorb.util;

import java.io.FileOutputStream;

import org.goldenorb.PartitionProcess;
import org.goldenorb.conf.OrbConfiguration;

public class MockPartitionThread implements PartitionProcess, Runnable {
  
  Thread thread;
  
  public MockPartitionThread() {
    thread = new Thread(this);
  }
  
  @Override
  public void run() {
    // TODO Auto-generated method stub
  }

  @Override
  public void launch(FileOutputStream outStream, FileOutputStream errStream) {
    // TODO Auto-generated method stub
    thread.start();
  }
  
  @Override
  public void kill() {
    // TODO Auto-generated method stub
    try {
      thread.join();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
  }
    
  @Override
  public OrbConfiguration getConf() {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public void setConf(OrbConfiguration conf) {
    // TODO Auto-generated method stub
    
  }
  
  @Override
  public String getIpAddress() {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public void setIpAddress(String ipAddress) {
    // TODO Auto-generated method stub
    
  }
  
  @Override
  public int getProcessNum() {
    // TODO Auto-generated method stub
    return 0;
  }
  
  @Override
  public void setProcessNum(int processNum) {
    // TODO Auto-generated method stub
    
  }
  
  @Override
  public boolean isRunning() {
    // TODO Auto-generated method stub
    return thread.isAlive();
  }
  
  @Override
  public void setRequestedPartitions(int requestedPartitions) {
    // TODO Auto-generated method stub
    
  }
  
  @Override
  public void setReservedPartitions(int reservedPartitions) {
    // TODO Auto-generated method stub
    
  }
}
