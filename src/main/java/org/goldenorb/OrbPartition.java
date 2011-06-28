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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.event.OrbCallback;
import org.goldenorb.event.OrbEvent;
import org.goldenorb.event.OrbExceptionEvent;
import org.goldenorb.io.InputSplitAllocator;
import org.goldenorb.io.input.RawSplit;
import org.goldenorb.io.input.VertexBuilder;
import org.goldenorb.io.output.OrbContext;
import org.goldenorb.io.output.VertexWriter;
import org.goldenorb.jet.OrbPartitionMember;
import org.goldenorb.net.OrbDNS;
import org.goldenorb.queue.InboundMessageQueue;
import org.goldenorb.queue.OutboundMessageQueue;
import org.goldenorb.queue.OutboundVertexQueue;
import org.goldenorb.zookeeper.AllDoneBarrier;
import org.goldenorb.zookeeper.Barrier;
import org.goldenorb.zookeeper.LeaderGroup;
import org.goldenorb.zookeeper.OrbFastAllDoneBarrier;
import org.goldenorb.zookeeper.OrbFastBarrier;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  OrbPartition, spawned from via {@link OrbPartitionProcess}, is responsible for loading
 *  input data, assigning file splits to other {@link OrbPartition} processes and coordinating with 
 *  other {@link OrbPartition} processes via the exchange of {@link Messages} and {@link Vertices}. 
 *  In addition to start up and coordination, {@link OrbPartition} processes are run responsible for 
 *  stepping through the graph algorithms themselves, via the compute method. 
 */
public class OrbPartition extends OrbPartitionMember implements Runnable, OrbPartitionCommunicationProtocol,
    OrbPartitionManagerProtocol {
  
  private final static int PARTITION_JOIN_TIMEOUT = 60000;
  
  private static Logger LOG = LoggerFactory.getLogger(OrbPartition.class);
  
  /**
   * The unique identifier for this partition.
   */
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
  
//  private String hostname;
  
  private boolean runPartition;
  
  private boolean loadedVerticesComplete = false;
  
  private Set<InputSplitLoaderHandler> inputSplitLoaderHandlers = Collections.synchronizedSet(new HashSet<InputSplitLoaderHandler>());
  private Set<MessagesHandler> messagesHandlers = Collections.synchronizedSet(new HashSet<MessagesHandler>());
  private Set<LoadVerticesHandler> loadVerticesHandlers = Collections.synchronizedSet(new HashSet<LoadVerticesHandler>());
  
  private InboundMessageQueue currentInboundMessageQueue;
  private InboundMessageQueue processingInboundMessageQueue;
  
  private OutboundMessageQueue outboundMessageQueue;
  
  private VoteToHaltSet processingVoteToHaltSet;
  
  private boolean hasMoreToProcess = true;
  
  private boolean computing = true;
  
  private ExecutorService inputSplitHandlerExecutor;
  private ExecutorService verticesLoaderHandlerExecutor;
  private ExecutorService messageHandlerExecutor;
  private ExecutorService computeExecutor;
  
  private Map<String,Vertex<?,?,?>> vertices = new HashMap<String, Vertex<?,?,?>>();
  
  private OrbCommunicationInterface oci = new OrbCommunicationInterface();
  
  Map<Integer,OrbPartitionCommunicationProtocol> orbClients;
  
  /**
   * Constructor
   * 
   * @param String
   *          jobNumber
   * @param int partitionID
   * @param boolean standby
   * @param int partitionBasePort
   */
  public OrbPartition(String jobNumber, int partitionID, boolean standby, int partitionBasePort) {
    this.setOrbConf(new OrbConfiguration(true));
    this.standby = standby;
    interpartitionCommunicationPort = partitionBasePort;
    trackerTetherCommunicationPort = partitionBasePort + 100;
    jobPath = "/GoldenOrb/" + getOrbConf().getOrbClusterName() + "/JobQueue/" + jobNumber;
    jobInProgressPath = "/GoldenOrb/" + getOrbConf().getOrbClusterName() + "/JobsInProgress/" + jobNumber;
    setPartitionID(partitionID);
    
    LOG.debug("Starting for job {}", jobInProgressPath);
    
    inputSplitHandlerExecutor = Executors.newFixedThreadPool(getOrbConf().getInputSplitHandlerThreads());
    messageHandlerExecutor = Executors.newFixedThreadPool(getOrbConf().getMessageHandlerThreads());
    computeExecutor = Executors.newFixedThreadPool(getOrbConf().getComputeThreads());
    verticesLoaderHandlerExecutor = Executors.newFixedThreadPool(getOrbConf()
        .getVerticesLoaderHandlerThreads());
    
    try {
      zk = ZookeeperUtils.connect(getOrbConf().getOrbZooKeeperQuorum());
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
      getOrbConf().setJobNumber(jobNumber);
      LOG.debug("setOrbConf with requested, reserved", jobConf.getOrbRequestedPartitions(), jobConf.getOrbReservedPartitions());
    }
    setSuperStep(0);
    setNumberOfVertices(0);
    setMessagesSent(0);
    setPercentComplete(0.0F);
    setLeader(false);     
  }
  
  /**
   * 
   * @param String
   *          [] args
   */
  public static void main(String[] args) {
    if (args.length != 4) {
      LOG.error("OrbPartition cannot start unless it is passed both the partitionID and the jobNumber to the Jobs OrbConfiguration");
    }

    LOG.debug("OrbPartition starting with args: {}", Arrays.toString(args));
    String jobNumber = args[0];
    int partitionID = Integer.parseInt(args[1]);
    boolean standby = Boolean.parseBoolean(args[2]);
    int partitionBasePort = Integer.parseInt(args[3]);
    new Thread(new OrbPartition(jobNumber, partitionID, standby, partitionBasePort)).start();
  }
  
  /**
 * 
 */
  @Override
  public void run() {
    
    try {
      setHostname(OrbDNS.getDefaultHost(getOrbConf()));
      setPort(trackerTetherCommunicationPort);
    } catch (UnknownHostException e) {
      LOG.error("Unable to get hostname.", e);
      System.exit(-1);
    }
    
    try {
      // TODO make this use the configuration to set this up
      interpartitionCommunicationServer = RPC.getServer(this, getHostname(),
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
      trackerTetherCommunicationServer = RPC.getServer(this, getHostname(),
        this.trackerTetherCommunicationPort, getOrbConf());
      trackerTetherCommunicationServer.start();
      LOG.info("Starting OrbPartition Tracker Tether Communication Server on: " + getHostname() + ":"
               + this.trackerTetherCommunicationPort);
    } catch (IOException e) {
      LOG.error("Failed to start Tracker Tether Communcation server!!", e);
      e.printStackTrace();
      System.exit(-1);
    }
    
    leaderGroup = new LeaderGroup<OrbPartitionMember>(zk, new OrbPartitionCallback(), jobInProgressPath + "/OrbPartitionLeaderGroup",
        this, OrbPartitionMember.class);
    
    LOG.debug("leaderGroup member paths {}", leaderGroup.getMembersPath().toString());
    LOG.debug("requested {}, reserved {}", getOrbConf().getOrbRequestedPartitions(), getOrbConf().getOrbReservedPartitions());
    
//    synchronized (this) {
//      while (leaderGroup.getNumOfMembers() < (getOrbConf().getOrbRequestedPartitions() + getOrbConf()
//          .getOrbReservedPartitions())) {
//        try {
//          LOG.debug("partition {} is waiting", getPartitionID());
//          wait(PARTITION_JOIN_TIMEOUT);
//        } catch (InterruptedException e) {
//          e.printStackTrace();
//        }
//      }
//    }
    
    enterBarrier("rdyToInitClientsBarrier");
    
    initializeOrbClients();
    
    if (leaderGroup.isLeader()) {
      executeAsLeader();
    } else {
      executeAsSlave();
    }
  }
  
  /**
 * 
 */
  private void initializeOrbClients() {
    orbClients = new HashMap<Integer,OrbPartitionCommunicationProtocol>();
    for (OrbPartitionMember orbPartitionMember : leaderGroup.getMembers()) {
      int count = 0;
      boolean connected = false;
      while(count < 20 && !connected){
        try {
          orbPartitionMember.initProxy(getOrbConf());
          connected = true;
          LOG.debug("partition {} proxy initialized for {}", getPartitionID(), orbPartitionMember.getPartitionID());
        } catch (IOException e) {
          count++;
          e.printStackTrace();
        }
      }
      orbClients.put(orbPartitionMember.getPartitionID(), orbPartitionMember);
    }
  }
  
  /**
 * 
 */
  private void executeAsSlave() {
    if (standby) {
      waitForActivate();
    }
    synchronized (this) {
      setLeader(false);
      if (!loadedVerticesComplete) {
        loadVerticesSlave();
      }
    }
    waitLoop();
  }
  
  /**
 * 
 */
  private void executeAsLeader() {
    synchronized (this) {
      setLeader(true);
      new Thread(new HeartbeatGenerator()).start();
      if (!loadedVerticesComplete) {
        loadVerticesLeader();
      }
    }
    waitLoop();
  }
  
  private void waitForActivate() {
    synchronized (this) {
      while (standby) {
        try {
          this.wait();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      // TODO need to have separate code for if it becomes active as a leader or as a slave
    }
  }
  
  /**
 * 
 */
  private void loadVerticesSlave() {
    enterBarrier("startLoadVerticesBarrier");
    
    // since we are a slave we immediately jump into this barrier
    enterBarrier("sentInputSplitsBarrier");
    
    // here we are handling our InputSplits by loading and sending vertices
    while (!inputSplitLoaderHandlers.isEmpty()) {
      synchronized (this) {
        try {
          wait(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    
    enterBarrier("inputSplitHandlersCompleteBarrier");
    
    // here we are handling all of the vertices that have been sent to us, and are loading them into vertices
    while (!loadVerticesHandlers.isEmpty()) {
      synchronized (this) {
        try {
          wait(1000);
          
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }
    
    enterBarrier("loadVerticesIntoPartitionBarrier");
    
    LOG.info("Partition " + getPartitionID() + " completed Loading vertices!!!");
    
    process();
    // try {
    // ZookeeperUtils.tryToCreateNode(zk, jobInProgressPath + "/messages/complete");
    // } catch (OrbZKFailure e) {
    // e.printStackTrace();
    // }
    // System.exit(1);
  }
  
  /**
   * This is where the core processing of the vertices -- and runnning of the algorithm lives.
   */
  private void process() {
    while (computing) {
      step();
      compute();
      LOG.info("Partition " + getPartitionID() + " back in run portion " + Integer.toString(getSuperStep()));
    }
  }
  
  public void compute() {
    
    if (getSuperStep() == 1) {
      enterBarrier("superStep1Barrier");
      
      processingVoteToHaltSet = new VoteToHaltSet(vertices.keySet());
      
      int count = 0;
      List<Vertex<?,?,?>> vertexList = new ArrayList<Vertex<?,?,?>>();
      List<List<Message<? extends Writable>>> messageList = new ArrayList<List<Message<? extends Writable>>>();
      int verticesLeft = vertices.keySet().size();
      for (Vertex<?,?,?> v : vertices.values()) {
//        count += 1;
//        verticesLeft -= 1;
//        vertexList.add(v);
//        messageList.add(new ArrayList<Message<? extends Writable>>());
//        
//        if (count >= getOrbConf().getVerticesPerBlock() || verticesLeft == 0) {
//          computeExecutor.execute(new VertexComputer(vertexList, messageList));
//          vertexList = new ArrayList<Vertex<?,?,?>>();
//          messageList = new ArrayList<List<Message<? extends Writable>>>();
//          count = 0;
//        }
        v.compute(new ArrayList());
      }
      synchronized (this) {
        while (!processingVoteToHaltSet.isEmpty()) {
          try {
            wait(1000);
            LOG.debug(Integer.toString(processingVoteToHaltSet.size()));

          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    } else {
      if (processingInboundMessageQueue.getVerticesWithMessages().size() == 0) {
        hasMoreToProcess = false;
        
        if (enterAllDoneBarrier("superStepBarrier", getSuperStep(), true)) {
          doneComputing();
        }
        
      } else {
        
        enterAllDoneBarrier("superStepBarrier", getSuperStep(), false);
        
        int count = 0;
        List<Vertex<?,?,?>> vertexList = new ArrayList<Vertex<?,?,?>>();
        List<List<Message<? extends Writable>>> messageList = new ArrayList<List<Message<? extends Writable>>>();
        int verticesLeft = processingInboundMessageQueue.getVerticesWithMessages().size();
        for (String s : processingInboundMessageQueue.getVerticesWithMessages()) {
//          count += 1;
//          verticesLeft -= 1;
//          vertexList.add(vertices.get(s));
//          messageList.add(processingInboundMessageQueue.getMessage(s));
//          
//          if (count >= getOrbConf().getVerticesPerBlock() || verticesLeft == 0) {
//            computeExecutor.execute(new VertexComputer(vertexList, messageList));
//            vertexList = new ArrayList<Vertex<?,?,?>>();
//            messageList = new ArrayList<List<Message<? extends Writable>>>();
//            count = 0;
//          }
          vertices.get(s).compute((Collection)processingInboundMessageQueue.getMessage(s));
        }
        synchronized (this) {
          while (!processingVoteToHaltSet.isEmpty()) {
            try {
              wait(10000);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            LOG.debug(Integer.toString(processingVoteToHaltSet.size()));
          }
        }
      }
    }
    
    enterBarrier("doneComputingVerticesBarrier", getSuperStep());
    
    outboundMessageQueue.sendRemainingMessages();
    
    enterBarrier("doneSendingMessagesBarrier", getSuperStep());
    
    LOG.info("Partition " + getPartitionID() + " going back to run portion " + Integer.toString(getSuperStep()));
  }
  
  private void doneComputing() {
    computing = false;
    LOG.info("Partition: (" + Integer.toString(getPartitionID()) + ") Done computing!!!!!!");
    dumpData();
    enterBarrier("doneDumpingDataBarrier");
    try {
      ZookeeperUtils.tryToCreateNode(zk, jobInProgressPath + "/messages/complete");
    } catch (OrbZKFailure e) {
      e.printStackTrace();
    }
    System.exit(1);
  }
  
  private void dumpData() {
    // TODO Auto-generated method stub
    Configuration conf = new Configuration();
    Job job = null;
    JobContext jobContext = null;
    TaskAttemptContext tao = null;
    RecordWriter rw;
    VertexWriter vw;
    FileOutputFormat outputFormat;
    
    boolean tryAgain = true;
    int count = 0;
    while (tryAgain && count < 15)
      try {
        count++;
        tryAgain = false;
        if (job == null) {
          job = new Job(conf);
          job.setOutputFormatClass(TextOutputFormat.class);
          FileOutputFormat.setOutputPath(job, new Path(new String(getOrbConf().getNameNode()
                                                                  + getOrbConf().getFileOutputPath())));
        }
        if (jobContext == null) {
          jobContext = new JobContext(job.getConfiguration(), new JobID());
        }
        
        System.out.println(jobContext.getConfiguration().get("mapred.output.dir"));
        
        tao = new TaskAttemptContext(jobContext.getConfiguration(), new TaskAttemptID(new TaskID(
            jobContext.getJobID(), true, getPartitionID()), 0));
        outputFormat = (FileOutputFormat) tao.getOutputFormatClass().newInstance();
        rw = outputFormat.getRecordWriter(tao);
        vw = (VertexWriter) getOrbConf().getVertexOutputFormatClass().newInstance();
        for (Vertex v : vertices.values()) {
          OrbContext oc = vw.vertexWrite(v);
          rw.write(oc.getKey(), oc.getValue());
          // orbLogger.info("Partition: " + Integer.toString(partitionId) + "writing: " +
          // oc.getKey().toString() + ", " + oc.getValue().toString());
        }
        rw.close(tao);
        
        FileOutputCommitter cm = (FileOutputCommitter) outputFormat.getOutputCommitter(tao);
        if (cm.needsTaskCommit(tao)) {
          cm.commitTask(tao);
          cm.cleanupJob(jobContext);
        } else {
          cm.cleanupJob(jobContext);
          tryAgain = true;
        }
        
      } catch (IOException e) {
        // TODO Auto-generated catch block
        tryAgain = true;
        e.printStackTrace();
      } catch (InstantiationException e) {
        // TODO Auto-generated catch block
        tryAgain = true;
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        // TODO Auto-generated catch block
        tryAgain = true;
        e.printStackTrace();
      } catch (ClassNotFoundException e) {
        // TODO Auto-generated catch block
        tryAgain = true;
        e.printStackTrace();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        tryAgain = true;
        e.printStackTrace();
      }
    if (tryAgain) {
      synchronized (this) {
        try {
          wait(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }
  
  class VertexComputer implements Runnable {
    private List<Vertex<?,?,?>> vertexList;
    private List<List<Message<? extends Writable>>> messageList;
    
    VertexComputer(List<Vertex<?,?,?>> vertexList, List<List<Message<? extends Writable>>> messageList) {
      this.vertexList = vertexList;
      this.messageList = messageList;
    }
    
    public void run() {
      for (int i = 0; i < vertexList.size(); i++) {
        vertexList.get(i).compute((Collection) messageList.get(i));
      }
      synchronized (OrbPartition.this) {
        if (processingVoteToHaltSet.isEmpty()) {
          OrbPartition.this.notify();
        }
      }
    }
  }
  
  public void step() {
    synchronized (this) {
      while (!messagesHandlers.isEmpty()) {
        try {
          wait(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      
      enterBarrier("messageHandlersDoneReceivingBarrier", getSuperStep());
      
      processingInboundMessageQueue = currentInboundMessageQueue;
      currentInboundMessageQueue = new InboundMessageQueue();
      try {
        outboundMessageQueue = new OutboundMessageQueue(getOrbConf().getOrbRequestedPartitions(),
            getOrbConf().getMessagesPerBlock(), orbClients,
            (Class<? extends Message<? extends Writable>>) getOrbConf().getMessageClass(), getPartitionID());
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
      setSuperStep(getSuperStep() + 1);
      LOG.info("********Starting SuperStep " + getSuperStep() + "Partition: " + getPartitionID() + "  *********");
      if (getSuperStep() > 1) {
        processingVoteToHaltSet = new VoteToHaltSet(processingInboundMessageQueue.getVerticesWithMessages());
      }
    }
  }
  
  /**
 * 
 */
  private void loadVerticesLeader() {
    enterBarrier("startLoadVerticesBarrier");
    
    // Here InputSplits are sent to their constituent partitions for loading
    InputSplitAllocator inputSplitAllocator = new InputSplitAllocator(getOrbConf(), leaderGroup.getMembers());
    Map<OrbPartitionMember,List<RawSplit>> inputSplitAssignments = inputSplitAllocator.assignInputSplits();
    for (OrbPartitionMember orbPartitionMember : inputSplitAssignments.keySet()) {
      for (RawSplit rawSplit : inputSplitAssignments.get(orbPartitionMember)) {
        orbPartitionMember.loadVerticesFromInputSplit(rawSplit);
      }
    }
    
    enterBarrier("sentInputSplitsBarrier");
    
    // just like the slave we have to wait for the InputSplitHandlers to finish loading and sending vertices
    while (!inputSplitLoaderHandlers.isEmpty()) {
      synchronized (this) {
        try {
          wait(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    
    enterBarrier("inputSplitHandlersCompleteBarrier");
    
    // just like the slave here we are handling all of the vertices that have been sent to us, and are loading
    // them into vertices
    while (!loadVerticesHandlers.isEmpty()) {
      synchronized (this) {
        try {
          wait(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    enterBarrier("loadVerticesIntoPartitionBarrier");
    
    LOG.debug("Completed Loading vertices!!!");
    
    if (standby) {
      waitForActivate();
    }
    
    process();
    // try {
    // ZookeeperUtils.tryToCreateNode(zk, jobInProgressPath + "/messages/complete");
    // } catch (OrbZKFailure e) {
    // e.printStackTrace();
    // }
    // System.exit(1);
  }
  
  /**
 * 
 */
  private void waitLoop() {
    while (runPartition) {
      synchronized (this) {
        try {
          wait();
        } catch (InterruptedException e) {
          LOG.error(e.getMessage());
        }
      }
      if ((leaderGroup.isLeader() && !isLeader()) || (!leaderGroup.isLeader() && isLeader())){
        if (leaderGroup.isLeader()) {
          executeAsLeader();
        } else {
          executeAsSlave();
        }
      }
    }
  }
  
  private class OrbPartitionCallback implements OrbCallback {
    
    /**
     * 
     * @param OrbEvent
     *          e
     */
    @Override
    public void process(OrbEvent e) {
      int eventCode = e.getType();
      if (eventCode == OrbEvent.ORB_EXCEPTION) {
        ((OrbExceptionEvent) e).getException().printStackTrace();
      } else if (eventCode == OrbEvent.LEADERSHIP_CHANGE) {
        synchronized (OrbPartition.this) {
          if ((leaderGroup.isLeader() && !isLeader()) || (!leaderGroup.isLeader() && isLeader())) {
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
    /**
     * 
     * @returns int
     */
    public int superStep() {
      return getSuperStep();
    }
    
    /**
     * 
     * @param String
     *          vertexID
     */
    public void voteToHalt(String vertexID) {
      processingVoteToHaltSet.voteToHalt(vertexID);
    }
    
    /**
     * 
     * @param Message
     *          <? extends Writable> message
     */
    public void sendMessage(Message<? extends Writable> message) {
      OrbPartition.this.outboundMessageQueue.sendMessage(message);
    }
  }
  
  /**
   * Return the protocolVersion
   */
  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return 0L;
  }
  
  /**
   * 
   * @returns int
   */
  @Override
  public int stop() {
    // TODO Shutdown stuff
    return 0;
  }
  
  /**
   * Return the unning
   */
  @Override
  public boolean isRunning() {
    // TODO what constitutes that it is no longer running?
    return true;
  }
  
  /**
   * 
   * @param Messages
   *          messages
   */
  @Override
  public void sendMessages(Messages messages) {
    MessagesHandler messagesHandler = new MessagesHandler(messages);
    messagesHandlers.add(messagesHandler);
    messageHandlerExecutor.execute(messagesHandler);
  }
  
  class MessagesHandler implements Runnable {
    private Messages messages;
    
    MessagesHandler(Messages messages) {
      this.messages = messages;
    }
    
    public void run() {
      synchronized (currentInboundMessageQueue) {
        currentInboundMessageQueue.addMessages(messages);
        
        synchronized (OrbPartition.this) {
          messagesHandlers.remove(this);
          LOG.info("Partition " + getPartitionID() + " " + OrbPartition.this + " messagesHandlerNotifying Parent "
                   + Integer.toString(getSuperStep()));
          OrbPartition.this.notify();
        }
      }
    }
  }
  
  /**
   * 
   * @param Vertices
   *          vertices
   */
  @Override
  public void sendVertices(Vertices vertices) {
    LoadVerticesHandler loadVerticesHandler = new LoadVerticesHandler(vertices, this);
    loadVerticesHandlers.add(loadVerticesHandler);
    verticesLoaderHandlerExecutor.execute(loadVerticesHandler);
  }
  
  class LoadVerticesHandler implements Runnable {
    private Vertices vertices;
    
    /**
     * Constructor
     * 
     * @param Vertices
     *          vertices
     * @param OrbPartition
     *          orbPartition
     */
    public LoadVerticesHandler(Vertices vertices, OrbPartition orbPartition) {
      this.vertices = vertices;
    }
    
    /**
 * 
 */
    public void run() {
      synchronized (vertices) {
        for (Vertex<?,?,?> vertex : vertices.getArrayList()) {
          vertex.setOci(oci);
          OrbPartition.this.vertices.put(vertex.getVertexID(), vertex);
        }
        LOG.info("( Partition: " + Integer.toString(getPartitionID()) + ") Loaded " + vertices.size()
                 + " vertices.");
      }
      loadVerticesHandlers.remove(this);
      synchronized (OrbPartition.this) {
        OrbPartition.this.notify();
      }
    }
  }
  
  /**
   * 
   * @param int partitionID
   */
  @Override
  public void becomeActive(int partitionID) {
    if (standby) {
      setPartitionID(partitionID);
      standby = false;
      synchronized (this) {
        notify();
      }
    }
  }
  
  /**
   * 
   * @param RawSplit
   *          rawsplit
   */
  @Override
  public void loadVerticesFromInputSplit(RawSplit rawsplit) {
    InputSplitLoaderHandler inputSplitLoaderHandler = new InputSplitLoaderHandler(rawsplit);
    inputSplitLoaderHandlers.add(inputSplitLoaderHandler);
    inputSplitHandlerExecutor.execute(inputSplitLoaderHandler);
  }
  
  class InputSplitLoaderHandler implements Runnable {
    private RawSplit rawsplit;
    
    /**
     * Constructor
     * 
     * @param RawSplit
     *          rawsplit
     */
    public InputSplitLoaderHandler(RawSplit rawsplit) {
      this.rawsplit = rawsplit;
    }
    
    /**
 * 
 */
    @SuppressWarnings("unchecked")
    @Override
    public void run() {
      
      OutboundVertexQueue outboundVertexQueue;
      outboundVertexQueue = new OutboundVertexQueue(getOrbConf().getOrbRequestedPartitions(), getOrbConf()
          .getVerticesPerBlock(), orbClients, (Class<? extends Vertex<?,?,?>>) getOrbConf().getVertexClass(),
          getPartitionID());
      
      LOG.info("Loading on machine " + getHostname() + ":" + interpartitionCommunicationPort);
      
      VertexBuilder<?,?,?> vertexBuilder = ReflectionUtils.newInstance(getOrbConf()
          .getVertexInputFormatClass(), getOrbConf());
      vertexBuilder.setOrbConf(getOrbConf());
      vertexBuilder.setPartitionID(getPartitionID());
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
    
    /**
 * 
 */
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
    
    /**
 * 
 */
    @Override
    public void kill() {
      active = false;
    }
    
    /**
 * 
 */
    @Override
    public void restart() {
      active = true;
    }
    
  }
  
  /**
   * 
   * @param String
   *          barrierName
   * @param int superStep
   */
  private void enterBarrier(String barrierName, int superStep) {
    enterBarrier(barrierName + Integer.toString(superStep));
  }
  
  /**
   * 
   * @param String
   *          barrierName
   */
  private void enterBarrier(String barrierName) {
    LOG.debug("creating barrier {}", barrierName);
    Barrier barrier = new OrbFastBarrier(getOrbConf(), jobInProgressPath + "/" + barrierName,
        leaderGroup.getNumOfMembers(), Integer.toString(getPartitionID()), zk);
    try {
      barrier.enter();
      LOG.debug("{} entered " + getPartitionID(), barrierName);
    } catch (OrbZKFailure e) {
      LOG.error("Failed to complete barrier: " + barrierName, e);
      e.printStackTrace();
    }
  }
  
  /**
   * @param boolean iAmDone
   * @param String
   *          barrierName
   * @param int superStep
   * @return
   */
  private boolean enterAllDoneBarrier(String barrierName, int superStep, boolean iAmDone) {
    return enterAllDoneBarrier(barrierName + Integer.toString(superStep), iAmDone);
  }
  
  /**
   * @param boolean iAmDone
   * @param String
   *          barrierName
   */
  private boolean enterAllDoneBarrier(String barrierName, boolean iAmDone) {
    AllDoneBarrier barrier = new OrbFastAllDoneBarrier(getOrbConf(), jobInProgressPath + "/" + barrierName,
        leaderGroup.getNumOfMembers(), Integer.toString(getPartitionID()), zk);
    try {
      return barrier.enter(iAmDone);
    } catch (OrbZKFailure e) {
      LOG.error("Failed to complete barrier: " + barrierName, e);
      e.printStackTrace();
    }
    return false;
  }
}
