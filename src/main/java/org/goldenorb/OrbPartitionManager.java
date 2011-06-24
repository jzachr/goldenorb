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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrbPartitionManager<M extends PartitionProcess> implements OrbConfigurable {
  
  private final Logger logger = LoggerFactory.getLogger(OrbPartitionManager.class);
  
  private List<M> childProcesses = new ArrayList<M>();
  private Class<M> processClass;
  private OrbConfiguration conf;
  private String ipAddress;
  
  // RPC client to be used for soft-killing OrbPartition processes
  private OrbPartitionManagerProtocol partitionClient;
  
  public OrbPartitionManager(OrbConfiguration conf, Class<M> clazz) {
    this.conf = conf;
    this.processClass = clazz;
    
    try {
      ipAddress = DNS.getDefaultHost(this.conf.getOrbLauncherNetworkDevice());
      if (ipAddress.endsWith(".")) {
        ipAddress = ipAddress.substring(0, ipAddress.length() - 1);
      }
      logger.debug("setting ipAddress to " + ipAddress);
    } catch (UnknownHostException e) {
      logger.error(e.getMessage());
    }
  }
  
  public void launchPartitions(int requested, int reserved) throws InstantiationException, IllegalAccessException {
    logger.info("requested " + requested + ", reserved " + reserved);
    for (int i = 0; i < (requested + reserved); i++) {
      M partition = processClass.newInstance();
      partition.setConf(conf);
      partition.setIpAddress(ipAddress);
      partition.setProcessNum(i);
//      partition.setRequestedPartitions(requested);
//      partition.setReservedPartitions(reserved);
      
      FileOutputStream outStream = null;
      FileOutputStream errStream = null;
      
      try {
        outStream = new FileOutputStream(new File(ipAddress + Integer.toString(3000 + i) + ".out"));
        errStream = new FileOutputStream(new File(ipAddress + Integer.toString(3000 + i) + ".err"));
      } catch (IOException e) {
        logger.error(e.getMessage());
      }
      logger.debug("launching partition process " + i + " on " + ipAddress);
      partition.launch(outStream, errStream);
      childProcesses.add(partition);
    }
  }
  
  public void stop() {
    Configuration rpcConf = new Configuration();
    for (M p : childProcesses) {
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
        logger.error(e.getMessage());
      } catch (InterruptedException e) {
        logger.error(e.getMessage());
      }
    }
  }
  
  public void kill() {
    for (M p : childProcesses) {
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
  
  public List<M> getChildProcesses() {
    return childProcesses;
  }

  public String getIpAddress() {
    return ipAddress;
  }

  public void setPartitionProcessClass(Class<M> partitionProcessClass) {
    this.processClass = partitionProcessClass;
  }

  public Class<M> getPartitionProcessClass() {
    return processClass;
  }
}
