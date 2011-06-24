package org.goldenorb;

import java.io.FileOutputStream;

import org.goldenorb.conf.OrbConfiguration;

public interface PartitionProcess {
  
  public void launch(FileOutputStream outStream, FileOutputStream errStream);
  
  public void kill();
  
//  public void setReserved(boolean reserved);
//  
//  public boolean isReserved();
  
  public OrbConfiguration getConf();
  
  public void setConf(OrbConfiguration conf);
  
  public String getIpAddress();
  
  public void setIpAddress(String ipAddress);
  
  public int getProcessNum();
  
  public void setProcessNum(int processNum);
  
  public boolean isRunning();
  
  public void setRequestedPartitions(int requestedPartitions);
  
  public void setReservedPartitions(int reservedPartitions);
  
}