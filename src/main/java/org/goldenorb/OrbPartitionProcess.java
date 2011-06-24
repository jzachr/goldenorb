package org.goldenorb;

import java.io.FileOutputStream;
import java.io.IOException;

import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.util.StreamWriter;

public class OrbPartitionProcess implements PartitionProcess {
  private Process process;
  private OrbConfiguration conf;
  private int processNum;
  private String ipAddress;
  private boolean reserved = false;
  private int requestedPartitions = 0;
  private int reservedPartitions = 0;
  
  public OrbPartitionProcess() {}
  
  public OrbPartitionProcess(OrbConfiguration conf, int processNum, String ipAddress) {
    this.conf = conf;
    this.processNum = processNum;
    this.ipAddress = ipAddress;
  }
  
  /* (non-Javadoc)
   * @see org.goldenorb.PartitionProcess#launch(java.io.FileOutputStream, java.io.FileOutputStream)
   */
  @Override
  public void launch(FileOutputStream outStream, FileOutputStream errStream) {
    // TODO Need to update Process launch arguments once OrbPartition is completed
    try {
      ProcessBuilder builder = new ProcessBuilder("java", conf.getOrbPartitionJavaopts(), "-cp",
          "goldenorb-0.0.1-SNAPSHOT-jar-with-dependencies.jar" + buildClassPathPart(),
          "org.goldenorb.OrbPartition", conf.getOrbJobName().toString(), conf.getOrbClusterName(), ipAddress,
          Integer.toString(conf.getOrbBasePort() + processNum), Integer.toString(requestedPartitions),
          Integer.toString(reservedPartitions));
      
      process = builder.start();
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
  
  /* (non-Javadoc)
   * @see org.goldenorb.PartitionProcess#kill()
   */
  @Override
  public void kill() {
    process.destroy();
  }

   /* (non-Javadoc)
   * @see org.goldenorb.PartitionProcess#getConf()
   */
  @Override
  public OrbConfiguration getConf() {
    return conf;
  }

  /* (non-Javadoc)
   * @see org.goldenorb.PartitionProcess#setConf(org.goldenorb.conf.OrbConfiguration)
   */
  @Override
  public void setConf(OrbConfiguration conf) {
    this.conf = conf;
  }

  /* (non-Javadoc)
   * @see org.goldenorb.PartitionProcess#getIpAddress()
   */
  @Override
  public String getIpAddress() {
    return ipAddress;
  }

  /* (non-Javadoc)
   * @see org.goldenorb.PartitionProcess#setIpAddress(java.lang.String)
   */
  @Override
  public void setIpAddress(String ipAddress) {
    this.ipAddress = ipAddress;
  }

  /* (non-Javadoc)
   * @see org.goldenorb.PartitionProcess#getProcessNum()
   */
  @Override
  public int getProcessNum() {
    return processNum;
  }

  /* (non-Javadoc)
   * @see org.goldenorb.PartitionProcess#setProcessNum(int)
   */
  @Override
  public void setProcessNum(int processNum) {
    this.processNum = processNum;
  }

  /* (non-Javadoc)
   * @see org.goldenorb.PartitionProcess#isRunning()
   */
  @Override
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

  /* (non-Javadoc)
   * @see org.goldenorb.PartitionProcess#setRequestedPartitions(int)
   */
  @Override
  public void setRequestedPartitions(int requestedPartitions) {
    this.requestedPartitions = requestedPartitions;
  }

  /* (non-Javadoc)
   * @see org.goldenorb.PartitionProcess#setReservedPartitions(int)
   */
  @Override
  public void setReservedPartitions(int reservedPartitions) {
    this.reservedPartitions = reservedPartitions;
  }
}
