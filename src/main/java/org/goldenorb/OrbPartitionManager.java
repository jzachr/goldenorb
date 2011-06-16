package org.goldenorb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.DNS;
import org.goldenorb.conf.OrbConfigurable;
import org.goldenorb.conf.OrbConfiguration;

public class OrbPartitionManager implements OrbConfigurable {
  
  private List<OrbPartitionProcess> childProcesses = new ArrayList<OrbPartitionProcess>();
  private OrbConfiguration conf;
  private String ipAddress;
  
  // RPC client to be used for soft-killing OrbPartition processes
  private OrbPartitionManagerProtocol partitionClient;
  
  public OrbPartitionManager(OrbConfiguration conf) {
    this.conf = conf;
    
    try {
      ipAddress = DNS.getDefaultHost(this.conf.getOrbLauncherNetworkDevice());
      if (ipAddress.endsWith(".")) {
        ipAddress = ipAddress.substring(0, ipAddress.length() - 1);
      }
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
  }
  
  public void launchPartitions() {
    for (int i = 0; i < conf.getNumberOfPartitionsPerMachine(); i++) {
      OrbPartitionProcess partitionProcess = new OrbPartitionProcess(conf, i, ipAddress);
      
      FileOutputStream outStream = null;
      FileOutputStream errStream = null;
      
      try {
        outStream = new FileOutputStream(new File(ipAddress + Integer.toString(3000 + i) + ".out"));
        errStream = new FileOutputStream(new File(ipAddress + Integer.toString(3000 + i) + ".err"));
      } catch (IOException e) {
        e.printStackTrace();
      }
      partitionProcess.launch(outStream, errStream);
      
      childProcesses.add(partitionProcess);
    }
  }
  
  public void stop() {
    Configuration rpcConf = new Configuration();
    for (OrbPartitionProcess p : childProcesses) {
      int rpcPort = conf.getOrbPartitionManagementBaseport() + p.getProcessNum();
      InetSocketAddress addr = new InetSocketAddress(ipAddress, rpcPort);
      try {
        partitionClient = (OrbPartitionManagerProtocol) RPC.waitForProxy(OrbPartitionManagerProtocol.class,
          OrbPartitionManagerProtocol.versionID, addr, rpcConf);
        
        int partitionStatus = partitionClient.stop();
        if (partitionStatus > 0) {
          // wait some time before trying to stop it again
          wait(5000);
          if (partitionClient.isRunning()) {
            p.kill();
          }
        } else if (partitionStatus < 0) {
          p.kill();
        }
        
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
  
  public void kill() {
    for (OrbPartitionProcess p : childProcesses) {
      p.kill();
    }
  }
  
  @Override
  public void setOrbConf(OrbConfiguration orbConf) {
    this.conf = orbConf;
  }
  
  @Override
  public OrbConfiguration getOrbConf() {
    return conf;
  }
  
  public List<OrbPartitionProcess> getChildProcesses() {
    return childProcesses;
  }

  public String getIpAddress() {
    return ipAddress;
  }
}
