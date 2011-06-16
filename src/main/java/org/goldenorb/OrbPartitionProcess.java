package org.goldenorb;

import java.io.FileOutputStream;
import java.io.IOException;

import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.util.StreamWriter;

public class OrbPartitionProcess {
  private Process process;
  private OrbConfiguration conf;
  private int processNum;
  private String ipAddress;
  private boolean reserved = false;
  
  public OrbPartitionProcess() {}
  
  public OrbPartitionProcess(OrbConfiguration conf, int processNum, String ipAddress) {
    this.conf = conf;
    this.processNum = processNum;
    this.ipAddress = ipAddress;
  }
  
  public void launch(FileOutputStream outStream, FileOutputStream errStream) {
    // TODO: Need to update Process launch arguments once OrbPartition is completed
    try {
      process = new ProcessBuilder("java", conf.getOrbPartitionJavaopts(), "-cp",
          "goldenorb-0.0.1-SNAPSHOT-jar-with-dependencies.jar" + buildClassPathPart(),
          "org.goldenorb.OrbPartition", conf.getOrbJobName().toString(), conf.getOrbClusterName(), ipAddress,
          Integer.toString(conf.getOrbBasePort() + processNum)).start();
      
      new StreamWriter(process.getErrorStream(), errStream);
      new StreamWriter(process.getInputStream(), outStream);
      
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  private String buildClassPathPart() {
    StringBuilder sb = new StringBuilder();
    for (String cp : conf.getOrbClassPaths()) {
      sb.append(":");
      sb.append(cp);
    }
    return sb.toString();
  }
  
  public void kill() {
    process.destroy();
  }

  public void setReserved(boolean reserved) {
    this.reserved = reserved;
  }

  public boolean isReserved() {
    return reserved;
  }

  public OrbConfiguration getConf() {
    return conf;
  }

  public void setConf(OrbConfiguration conf) {
    this.conf = conf;
  }

  public String getIpAddress() {
    return ipAddress;
  }

  public void setIpAddress(String ipAddress) {
    this.ipAddress = ipAddress;
  }

  public int getProcessNum() {
    return processNum;
  }

  public void setProcessNum(int processNum) {
    this.processNum = processNum;
  }

  public boolean isRunning() {
    boolean ret = false;
    try {
      process.exitValue();
    } catch(IllegalThreadStateException e) {
      e.printStackTrace();
      ret = true;
    }
    
    return ret;
  }
}
