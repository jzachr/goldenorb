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
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.event.OrbCallback;
import org.goldenorb.event.OrbEvent;
import org.goldenorb.event.OrbExceptionEvent;
import org.goldenorb.io.input.RawSplit;
import org.goldenorb.io.input.VertexBuilder;
import org.goldenorb.jet.OrbPartitionMember;
import org.goldenorb.net.OrbDNS;
import org.goldenorb.queue.OutboundVertexQueue;
import org.goldenorb.zookeeper.Barrier;
import org.goldenorb.zookeeper.LeaderGroup;
import org.goldenorb.zookeeper.OrbBarrier;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrbPartition extends OrbPartitionMember implements Runnable, OrbPartitionCommunicationProtocol,
    OrbPartitionManagerProtocol {
  
  private final static int PARTITION_JOIN_TIMEOUT = 60000;
  
  private static Logger LOG = LoggerFactory.getLogger(OrbPartition.class);
  
  /**
   * The unique identifier for this partition.
   */
  private int partitionID;
  private boolean leader = false;
  private String jobPath;
  private String jobInProgressPath;
  
  private ZooKeeper zk;
  
  private boolean standby;
  
  private boolean waitingForAllToJoin = true;
  
  private LeaderGroup<OrbPartitionMember> leaderGroup;
  
  private Server interpartitionCommunicationServer;
  private Server trackerTetherCommunicationServer;
  
  private int interpartitionCommunicationPort;
  private int trackerTetherCommunicationPort;
  
  private String hostname;
  
  private boolean runPartition;
  
  private HeartbeatGenerator heartbeatGenerator;
  
  private boolean loadedVerticesComplete = false;
  
  private Set<InputSplitLoaderHandler> inputSplitLoaderHandlers = new HashSet<InputSplitLoaderHandler>();
  private Set<LoadVerticesHandler> loadVerticesHandlers = new HashSet<LoadVerticesHandler>();
  
  private ExecutorService inputSplitHandlerExecutor;
  private ExecutorService verticesLoaderHandlerExecutor;
  private ExecutorService messageHandlerExecutor;
  private ExecutorService computeExecutor;
  
  private Map<String, Vertex<?,?,?>> vertices;
  
  private OrbCommunicationInterface oci = new OrbCommunicationInterface();
  
  Map<Integer,OrbPartitionCommunicationProtocol> orbClients;
  
  public OrbPartition(String jobNumber, int partitionID, boolean standby, int partitionBasePort) {
    this.setOrbConf(new OrbConfiguration(true));
    this.standby = standby;
    interpartitionCommunicationPort = partitionBasePort;
    trackerTetherCommunicationPort = partitionBasePort + 100;
    jobPath = "/GoldenOrb/" + getOrbConf().getOrbClusterName() + "/JobQueue/" + jobNumber;
    jobInProgressPath = "/GoldenOrb/" + getOrbConf().getOrbClusterName() + "/JobsInProgress/" + jobNumber;
    this.partitionID = partitionID;
    
    inputSplitHandlerExecutor = Executors.newFixedThreadPool(getOrbConf().getInputSplitHandlerThreads());
    messageHandlerExecutor = Executors.newFixedThreadPool(getOrbConf().getMessageHandlerThreads());
    computeExecutor = Executors.newFixedThreadPool(getOrbConf().getComputeThreads());
    verticesLoaderHandlerExecutor = Executors.newFixedThreadPool(getOrbConf()
        .getVerticesLoaderHandlerThreads());
    
    try {
      ZookeeperUtils.connect(getOrbConf().getOrbZooKeeperQuorum());
    } catch (Exception e) {
      LOG.error("Unable to establish a connection with ZooKeeper" + getOrbConf().getOrbZooKeeperQuorum(), e);
      System.exit(-1);
    }
    OrbConfiguration jobConf = null;
    try {
      jobConf = (OrbConfiguration) ZookeeperUtils.getNodeWritable(zk, jobPath, OrbConfiguration.class,
        getOrbConf());
    } catch (OrbZKFailure e) {
      LOG.error("Unable to retrieve job from ZooKeeper: " + jobPath, e);
      System.exit(-1);
    }
    if (jobConf != null) {
      setOrbConf(jobConf);
    }
  }
  
  public static void main(String[] args) {
    if (args.length != 4) {
      LOG.error("OrbPartition cannot start unless it is passed both the partitionID and the jobNumber to the Jobs OrbConfiguration");
    }
    String jobNumber = args[0];
    int partitionID = Integer.parseInt(args[1]);
    boolean standby = Boolean.parseBoolean(args[2]);
    int partitionBasePort = Integer.parseInt(args[3]);
    new OrbPartition(jobNumber, partitionID, standby, partitionBasePort);
  }
  
  @Override
  public void run() {
    
    try {
      setHostname(OrbDNS.getDefaultHost(getOrbConf()));
    } catch (UnknownHostException e) {
      LOG.error("Unable to get hostname.", e);
      System.exit(-1);
    }
    
    try {
      // TODO make this use the configuration to set this up
      interpartitionCommunicationServer = RPC.getServer(this, this.hostname,
        this.interpartitionCommunicationPort, 10, false, getOrbConf());
      interpartitionCommunicationServer.start();
      LOG.info("Starting OrbPartition Interpartition Communication Server on: " + getHostname() + ":"
               + this.interpartitionCommunicationPort);
    } catch (IOException e) {
      LOG.error("Failed to start OrbPartition Interpartition Communication server!!", e);
      e.printStackTrace();
      System.exit(-1);
    }
    
    try {
      trackerTetherCommunicationServer = RPC.getServer(this, this.hostname,
        this.trackerTetherCommunicationPort, getOrbConf());
      trackerTetherCommunicationServer.start();
      LOG.info("Starting OrbPartition Tracker Tether Communication Server on: " + getHostname() + ":"
               + this.trackerTetherCommunicationPort);
    } catch (IOException e) {
      LOG.error("Failed to start Tracker Tether Communcation server!!", e);
      e.printStackTrace();
      System.exit(-1);
    }
    
    leaderGroup = new LeaderGroup<OrbPartitionMember>(zk, new OrbPartitionCallback(), jobInProgressPath,
        this, OrbPartitionMember.class);
    synchronized (this) {
      while (leaderGroup.getNumOfMembers() < (getOrbConf().getOrbRequestedPartitions() + getOrbConf()
          .getOrbReservedPartitions())) {
        try {
          wait(PARTITION_JOIN_TIMEOUT);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    initializeOrbClients();
    // Loop waiting for it to become active. If it is already "active" then it should blow right through this.
    // i.e. if it is in this loop then it is waiting
    
    // synchronized(this){
    // while(standby){
    // LOG.info("OrbPartition " + partitionID + "is Standby partition.  Waiting for failure!");
    // try {
    // this.wait();
    // } catch (InterruptedException e) {
    // e.printStackTrace();
    // }
    // }
    // }
    
    synchronized (this) {
      while (standby) {
        try {
          this.wait();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }
  
  private void initializeOrbClients() {
    orbClients = new HashMap<Integer,OrbPartitionCommunicationProtocol>();
    for (OrbPartitionMember orbPartitionMember : leaderGroup.getMembers()) {
      try {
        orbPartitionMember.initProxy(getOrbConf());
      } catch (IOException e) {
        // TODO This is a significant error and should start the killing of the partition
        e.printStackTrace();
      }
      orbClients.put(orbPartitionMember.getPartitionID(), orbPartitionMember);
    }
  }
  
  private void executeAsSlave() {
    synchronized (this) {
      leader = false;
      if (!loadedVerticesComplete) {
        loadVerticesSlave();
      }
    }
    waitLoop();
  }
  
  private void executeAsLeader() {
    synchronized (this) {
      leader = true;
      if (!loadedVerticesComplete) {
        loadVerticesLeader();
      }
      heartbeatGenerator = new HeartbeatGenerator();
    }
    waitLoop();
  }
  
  private void loadVerticesSlave() {
    enterBarrier("startLoadVerticesBarrier");
    enterBarrier("sentInputSplitsBarrier");
    
    while(!inputSplitLoaderHandlers.isEmpty()){
      synchronized(this){
        try {
          wait(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    
    enterBarrier("inputSplitHandlersCompleteBarrier");

    while(!loadVerticesHandlers.isEmpty()){
      synchronized(this){
        try {
          wait(1000);
          
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }
    
    enterBarrier("loadVerticesIntoPartitionBarrier");
  }
  
  private void loadVerticesLeader() {
    enterBarrier("startLoadVerticesBarrier");
    // TODO start sending inputsplits to the machines that need to process them
    enterBarrier("sentInputSplitsBarrier");
    enterBarrier("inputSplitHandlersCompleteBarrier");
    enterBarrier("loadVerticesIntoPartitionBarrier");
  }
  
  private void waitLoop() {
    while (runPartition) {
      synchronized (this) {
        try {
          wait();
        } catch (InterruptedException e) {
          LOG.error(e.getMessage());
        }
      }
      if (leaderGroup.isLeader()) {
        executeAsLeader();
      } else {
        executeAsSlave();
      }
    }
  }
  
  private class OrbPartitionCallback implements OrbCallback {
    
    @Override
    public void process(OrbEvent e) {
      int eventCode = e.getType();
      if (eventCode == OrbEvent.ORB_EXCEPTION) {
        ((OrbExceptionEvent) e).getException().printStackTrace();
      } else if (eventCode == OrbEvent.LEADERSHIP_CHANGE) {
        synchronized (OrbPartition.this) {
          if ((leaderGroup.isLeader() && !leader) || (!leaderGroup.isLeader() && leader)) {
            OrbPartition.this.notify();
          }
        }
      } else if (eventCode == OrbEvent.NEW_MEMBER) {
        synchronized (OrbPartition.this) {
          if (waitingForAllToJoin) {
            OrbPartition.this.notify();
          }
        }
      }
    }
    
  }
  
  public class OrbCommunicationInterface {
    public int superStep() {
      return 0;
    }
    
    public void voteToHalt(String vertexID) {}
    
    public void sendMessage(Message<? extends Writable> message) {}
  }
  
  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return 0L;
  }
  
  @Override
  public int stop() {
    // TODO Shutdown stuff
    return 0;
  }
  
  @Override
  public boolean isRunning() {
    // TODO what constitutes that it is no longer running?
    return true;
  }
  
  @Override
  public void sendMessages(Messages messages) {
    // TODO this should create a new handler to handle the inbound messages.
    
  }
  
  @Override
  public void sendVertices(Vertices vertices) {
    LoadVerticesHandler loadVerticesHandler = new LoadVerticesHandler(vertices, this);
    loadVerticesHandlers.add(loadVerticesHandler);
    verticesLoaderHandlerExecutor.execute(loadVerticesHandler);
  }
  
  class LoadVerticesHandler implements Runnable {
    private Vertices vertices;
    
    public LoadVerticesHandler(Vertices vertices, OrbPartition orbPartition) {
      this.vertices = vertices;
    }
    
    public void run() {
      synchronized (vertices) {
        for (Vertex<?,?,?> vertex : vertices.getArrayList()) {
          vertex.setOci(oci);
          OrbPartition.this.vertices.put(vertex.getVertexID(), vertex);
        }
        LOG.info("( Partition: " + Integer.toString(partitionID) + ") Loaded " + vertices.size()
                 + " vertices.");
      }
      loadVerticesHandlers.remove(this);
      synchronized (OrbPartition.this) {
        OrbPartition.this.notify();
      }
    }
  }
  
  @Override
  public void becomeActive(int partitionID) {
    if (standby) {
      this.partitionID = partitionID;
      standby = false;
      synchronized (this) {
        notify();
      }
    }
  }
  
  @Override
  public void loadVerticesFromInputSplit(RawSplit rawsplit) {
    InputSplitLoaderHandler inputSplitLoaderHandler = new InputSplitLoaderHandler(rawsplit);
    inputSplitLoaderHandlers.add(inputSplitLoaderHandler);
    inputSplitHandlerExecutor.execute(inputSplitLoaderHandler);
  }
  
  class InputSplitLoaderHandler implements Runnable {
    private RawSplit rawsplit;
    
    public InputSplitLoaderHandler(RawSplit rawsplit) {
      this.rawsplit = rawsplit;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void run() {
      
      OutboundVertexQueue outboundVertexQueue;
      outboundVertexQueue = new OutboundVertexQueue(getOrbConf().getOrbRequestedPartitions(), getOrbConf()
          .getNumberOfVerticesPerBlock(), orbClients, (Class<? extends Vertex<?,?,?>>) getOrbConf()
          .getVertexClass(), partitionID);
      
      LOG.info("Loading on machine " + hostname + ":" + interpartitionCommunicationPort);
      
      VertexBuilder<?,?,?> vertexBuilder = ReflectionUtils.newInstance(getOrbConf()
          .getVertexInputFormatClass(), getOrbConf());
      vertexBuilder.setPartitionID(1);
      vertexBuilder.setRawSplit(rawsplit.getBytes());
      vertexBuilder.setSplitClass(rawsplit.getClassName());
      vertexBuilder.initialize();
      
      try {
        while (vertexBuilder.nextVertex()) {
          outboundVertexQueue.sendVertex(vertexBuilder.getCurrentVertex());
        }
      } catch (IOException e) {
        // TODO Data loading failed --- needs to fire a death event.
        e.printStackTrace();
      } catch (InterruptedException e) {
        // TODO Data loading failed --- needs to fire a death event.
        e.printStackTrace();
      }
      
      outboundVertexQueue.sendRemainingVertices();
      inputSplitLoaderHandlers.remove(this);
      synchronized (OrbPartition.this) {
        OrbPartition.this.notify();
      }
    }
  }
  
  private class HeartbeatGenerator implements Runnable, Killable {
    
    private boolean active = true;
    private Long heartbeat = 1L;
    
    @Override
    public void run() {
      while (active) {
        synchronized (this) {
          try {
            wait((getOrbConf().getJobHeartbeatTimeout() / 10));
            try {
              ZookeeperUtils.existsUpdateNodeData(zk, jobInProgressPath + "/messages/heartbeat",
                new LongWritable(heartbeat++));
              LOG.debug("Creating heartbeat for: " + jobInProgressPath + "/messages/heartbeat"
                        + " heartbeat is: " + heartbeat);
            } catch (OrbZKFailure e) {
              e.printStackTrace();
            }
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    }
    
    @Override
    public void kill() {
      active = false;
    }
    
    @Override
    public void restart() {
      active = true;
    }
    
  }
  
  private void enterBarrier(String barrierName, int superStep) {
    enterBarrier(barrierName + Integer.toString(superStep));
  }
  
  private void enterBarrier(String barrierName) {
    Barrier barrier = new OrbBarrier(getOrbConf(), jobInProgressPath + "/" + barrierName,
        leaderGroup.getNumOfMembers(), Integer.toString(partitionID), zk);
    try {
      barrier.enter();
    } catch (OrbZKFailure e) {
      LOG.error("Failed to complete barrier: " + barrierName, e);
      e.printStackTrace();
    }
  }
  
}
