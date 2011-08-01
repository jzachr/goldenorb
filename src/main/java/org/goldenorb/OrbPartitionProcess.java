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
 */
package org.goldenorb;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.util.StreamWriter;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link OrbPartitionProcess} is the {@link OrbPartition} process launcher. It sets the command-line
 * arguments and output/error streams for each process.
 */
public class OrbPartitionProcess implements PartitionProcess {
  private Process process;
  private OrbConfiguration conf;
  private OrbConfiguration jobConf;
  private int processNum;
  private boolean reserved = false;
  private int partitionID;
  private String jobNumber;
  
  private final Logger logger = LoggerFactory.getLogger(OrbPartitionProcess.class);
  
  /**
   * Constructor
   * 
   */
  public OrbPartitionProcess() {}
  
  /**
   * Constructor
   * 
   * @param OrbConfiguration
   *          conf
   * @param int processNum
   * @param boolean reserved
   * @param int partitionID
   */
  public OrbPartitionProcess(OrbConfiguration conf, OrbConfiguration jobConf, int processNum, boolean reserved, int partitionID) {
    this.conf = conf;
    this.processNum = processNum;
    this.reserved = reserved;
    this.partitionID = partitionID;
    this.jobConf = jobConf;
  }
  
  /**
   * 
   * @param FileOutputStream
   *          outStream
   * @param FileOutputStream
   *          errStream
   */
  @Override
  public void launch(OutputStream outStream, OutputStream errStream) {
    try {
      String customClassPath = buildClassPathPart();
      int orbBasePort = conf.getOrbBasePort();
      String tmp = jobConf.getOrbPartitionJavaopts();
      String[] orbPartitionJavaopts = tmp != null ? tmp.split(" ") : new String[0];
      String sysClassPath = System.getProperties().getProperty("java.class.path", null);
      String debugOpts = jobConf.getPartitionDebug() ? setupDebugOptions() : new String();
      
      List<String> args = new ArrayList<String>();
      
      args.add(System.getProperty("java.home") + "/bin/java");
      
      if(debugOpts.length() > 0) {
        args.add(setupDebugOptions());
      }

      args.addAll(Arrays.asList(orbPartitionJavaopts));
      args.add("-cp");
      args.add(sysClassPath + customClassPath);
      args.add("org.goldenorb.OrbPartition");
      args.add(jobNumber);
      args.add(Integer.toString(partitionID));
      args.add(Boolean.toString(reserved));
      args.add(Integer.toString(orbBasePort + processNum));

      logger.debug("process args: {}", args.toString());
      
      ProcessBuilder builder = new ProcessBuilder();
      builder.command(args);
      builder.environment().clear();
      
      process = builder.start();
      
//      Runtime runtime = Runtime.getRuntime();
//      process = runtime.exec(args.toArray(new String[args.size()]), new String[0]);
      
      new StreamWriter(process.getErrorStream(), errStream);
      new StreamWriter(process.getInputStream(), outStream);
      
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (OrbZKFailure e) {
      logger.error("Exception occured while adding distributed files to the class path");
      e.printStackTrace();
    }
  }
  
  /**
   * 
   * @throws InterruptedException
   * @throws IOException
   * @throws OrbZKFailure
   * @returns String
   */
  private String buildClassPathPart() throws IOException, InterruptedException, OrbZKFailure {
    StringBuilder sb = new StringBuilder();
    String[] orbClassPaths = conf.getOrbClassPaths();
    if (orbClassPaths != null) {
      for (String cp : orbClassPaths) {
        sb.append(":");
        sb.append(cp);
      }
    }
    
    List<String> localFilesPath = getLocalFilesPath();
    if (localFilesPath != null) {
      for (String cp : localFilesPath) {
        sb.append(":");
        sb.append(cp);
      }
    }
    
    return sb.toString();
  }
  
  /**
   * Calculates what the class paths for the distributed files that are in this partitions temp directory. All
   * files are distributed to /temp directory/GoldenOrb/jobNumber/file name.
   * 
   * @return A list of the class paths that need to be added to this partitions class path
   * @throws IOException
   * @throws InterruptedException
   * @throws OrbZKFailure
   */
  private List<String> getLocalFilesPath() throws IOException, InterruptedException, OrbZKFailure {
    
    List<String> paths = null;
    ZooKeeper zk = ZookeeperUtils.connect(conf.getOrbZooKeeperQuorum());
    String zkPath = "/GoldenOrb/" + conf.getOrbClusterName() + "/JobQueue/" + jobNumber;
    logger.info("Getting node " + zkPath + " from ZooKeeper");
    OrbConfiguration orbConf = (OrbConfiguration) ZookeeperUtils.getNodeWritable(zk, zkPath,
      OrbConfiguration.class, conf);
    if (orbConf != null) {
      Path[] tempPaths = orbConf.getHDFSdistributedFiles();
      if (tempPaths != null) {
        paths = new ArrayList<String>();
        for (Path path : tempPaths) {
          String[] name = path.toString().split("/");
          // files are always put in /<temp directory>/GoldenOrb/<jobNumber>/<file name>
          paths.add(System.getProperty("java.io.tmpdir") + "/GoldenOrb/" + orbConf.getOrbClusterName() + "/"
                    + jobNumber + "/" + name[name.length - 1]);
        }
      }
    }
    zk.close();
    return paths;
    
  }
  
  /**
 * 
 */
  @Override
  public void kill() {
    process.destroy();
  }
  
  /**
   * Return the conf
   */
  public OrbConfiguration getConf() {
    return conf;
  }
  
  /**
   * Set the conf
   * 
   * @param OrbConfiguration
   *          conf
   */
  @Override
  public void setConf(OrbConfiguration conf) {
    this.conf = conf;
  }
  
  /**
   * Return the processNum
   */
  @Override
  public int getProcessNum() {
    return processNum;
  }
  
  /**
   * Set the processNum
   * 
   * @param int processNum
   */
  @Override
  public void setProcessNum(int processNum) {
    this.processNum = processNum;
  }
  
  /**
   * Return the unning
   */
  @Override
  public boolean isRunning() {
    boolean ret = false;
    try {
      process.exitValue();
    } catch (IllegalThreadStateException e) {
      e.printStackTrace();
      ret = true;
    }
    
    return ret;
  }
  
  /**
   * Set the reserved
   * 
   * @param boolean reserved
   */
  @Override
  public void setReserved(boolean reserved) {
    this.reserved = reserved;
  }
  
  /**
   * Return the eserved
   */
  @Override
  public boolean isReserved() {
    return reserved;
  }
  
  /**
   * Set the partitionID
   * 
   * @param int partitionID
   */
  @Override
  public void setPartitionID(int partitionID) {
    this.partitionID = partitionID;
  }
  
  /**
   * Return the partitionID
   */
  @Override
  public int getPartitionID() {
    return partitionID;
  }
  
  @Override
  public void setJobNumber(String jobNumber) {
    this.jobNumber = jobNumber;
  }
  
  @Override
  public String getJobNumber() {
    return jobNumber;
  }
  
  private String setupDebugOptions() {
    StringBuilder sb = new StringBuilder();
    String ids = jobConf.getPartitionDebugIds();
    boolean debugThisPartition = false;
    
    if(ids.equalsIgnoreCase("*")) {
      debugThisPartition = true;
    }
    else {
      for(String id : ids.split(",")) {
        if(Integer.parseInt(id) == partitionID) {
          debugThisPartition = true;
          break;
        }
      }
    }
    
    if(debugThisPartition) {
      int debugPort = jobConf.getPartitionDebugBaseport() + partitionID;
      sb.append("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=localhost:");
      sb.append(debugPort);
    }
    
    return sb.toString();
  }

  @Override
  public void setCurrentJobConf(OrbConfiguration jobConf) {
    this.jobConf = jobConf;
    
  }

  @Override
  public OrbConfiguration getCurrentJobConf() {
    return jobConf;
  }
}
