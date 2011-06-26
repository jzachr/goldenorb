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
 * 
 */
package org.goldenorb.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.goldenorb.Vertex;
import org.goldenorb.io.input.VertexBuilder;
import org.goldenorb.io.output.VertexWriter;

public class OrbConfiguration extends Configuration {
  
  public static final String ORB_CLUSTER_BASEPORT = "goldenOrb.cluster.baseport";
  public static final String ORB_CLUSTER_NAME = "goldenOrb.cluster.name";
  public static final String ORB_CLASS_PATHS = "goldenOrb.orb.classpaths";
  public static final String ORB_TRACKER_PORT = "goldenOrb.orb.tracker.port";

  public static final String ORB_JOB_NUMBER = "goldenOrb.job.number";
  public static final String ORB_JOB_NAME = "goldenOrb.job.name";
  public static final String ORB_JOB_HEARTBEAT_TIMEOUT = "goldenOrb.job.heartbeatTimeout";
  public static final String ORB_JOB_MAX_TRIES = "goldenOrb.job.max.tries";
  
  public static final String ORB_ZOOKEEPER_QUORUM = "goldenOrb.zookeeper.quorum";
  public static final String ORB_ZOOKEEPER_PORT = "goldenOrb.zookeeper";
  
  public static final String ORB_PARTITIONS_PER_MACHINE = "goldenOrb.orb.partitionsPerMachine";
  public static final String ORB_REQUESTED_PARTITIONS = "goldenOrb.orb.requestedPartitions";
  public static final String ORB_RESERVED_PARTITIONS = "goldenOrb.orb.reservedPartitions";
  public static final String ORB_PARTITION_VERTEX_THREADS = "goldenOrb.orb.partition.vertex.threads";
  public static final String ORB_PARTITION_MESSAGEHANDLER_THREADS = "goldenOrb.orb.partition.messagehandlers.threads";
  public static final String ORB_PARTITION_JAVAOPTS = "goldenOrb.orb.partition.javaopts";
  public static final String ORB_PARTITION_MANAGEMENT_BASEPORT = "goldenOrb.orb.partitionManagement.baseport";
  
  public static final String ORB_LAUNCHER = "goldenOrb.orb.launcher";
  public static final String ORB_LAUNCHER_NETWORKDEVICE = "goldenOrb.orb.launcher.networkDevice";
  
  public static final String ORB_VERTEX_CLASS = "goldenOrb.orb.vertexClass";
  public static final String ORB_MESSAGE_CLASS = "goldenOrb.orb.messageClass";
  public static final String ORB_VERTEX_INPUT_FORMAT_CLASS = "goldenOrb.orb.vertexInputFormatClass";
  public static final String ORB_VERTEX_OUTPUT_FORMAT_CLASS = "goldenOrb.orb.vertexOutputFormatClass";
  public static final String ORB_NUMBER_VERTICES_BLOCK = "goldenOrb.orb.verticesPerBlock";
  public static final String ORB_NUMBER_MESSAGES_BLOCK = "goldenOrb.orb.messagesPerBlock";
  public static final String ORB_HANDLERS_PER_RPC_SERVER = "goldenOrb.orb.handlersPerServer";

  public static final String ORB_INPUT_SPLIT_HANDLER_THREADS = "goldenOrb.orb.inputSplitHandlerThreads";
  public static final String ORB_MESSAGE_HANDLER_THREADS = "goldenOrb.orb.messageHandlerThreads";
  public static final String ORB_COMPUTE_THREADS = "goldenOrb.orb.computeThreads";
  public static final String ORB_VERTICES_HANDLER_THREADS = "goldenOrb.orb.vertexHandlerThreads";
  
  public static final String ORB_ERROR_OUTPUT_STREAM = "goldenOrb.error.output.stream";
  public static final String ORB_SYSTEM_OUTPUT_STREAM = "goldenOrb.system.output.stream";
  
  public static final String ORB_FS_DEFAULT_NAME = "fs.default.name";
  public static final String ORB_FILE_INPUT_FORMAT_CLASS = "mapreduce.inputformat.class";
  public static final String ORB_FILE_OUTPUT_FORMAT_CLASS = "mapreduce.outputformat.class";
  public static final String ORB_FILE_INPUT_DIR = "mapred.input.dir";
  public static final String ORB_FILE_OUTPUT_DIR = "mapred.output.dir";
  
/**
 * Constructor
 *
 */
  public OrbConfiguration() {}
  
/**
 * Constructor
 *
 * @param  boolean loadDefaults
 */
  public OrbConfiguration(boolean loadDefaults) {
    
    super(loadDefaults);
    if (loadDefaults) this.addOrbResources((Configuration) this);
    else {
      // need the file to load if not defaults
    }
  }
  
/**
 * 
 * @param  Configuration conf
 * @returns Configuration
 */
  private static Configuration addOrbResources(Configuration conf) {
    conf.addDefaultResource("orb-default.xml");
    conf.addDefaultResource("orb-site.xml");
    return conf;
  }
  
/**
 * 
 * @param  Object rhs
 * @returns boolean
 */
  @Override
  public boolean equals(Object rhs) {
    return this.getJobNumber().equals(((OrbConfiguration) rhs).getJobNumber());
  }

/**
 * Return the messageClass
 */
  public Class<?> getMessageClass() throws ClassNotFoundException {
    return Class.forName(this.get(this.ORB_MESSAGE_CLASS));
  }
  
/**
 * Set the messageClass
 * @param  Class<?> messageClass
 */
  public void setMessageClass(Class<?> messageClass) {
    this.set(this.ORB_MESSAGE_CLASS, messageClass.getCanonicalName());
  }
  
/**
 * Return the vertexOutputFormatClass
 */
  public Class<? extends VertexWriter> getVertexOutputFormatClass() {
    return (Class<? extends VertexWriter>) this.getClass(this.ORB_VERTEX_OUTPUT_FORMAT_CLASS,
      VertexWriter.class);
  }
  
/**
 * Set the vertexOutputFormatClass
 * @param  Class<?> vertexOutputFormatClass
 */
  public void setVertexOutputFormatClass(Class<?> vertexOutputFormatClass) {
    this.set(this.ORB_VERTEX_OUTPUT_FORMAT_CLASS, vertexOutputFormatClass.getCanonicalName());
  }
  
/**
 * Return the fileOutputPath
 */
  public String getFileOutputPath() {
    return this.get(this.ORB_FILE_OUTPUT_DIR);
  }
  
/**
 * Set the fileOutputPath
 * @param  String fileOutputPath
 */
  public void setFileOutputPath(String fileOutputPath) {
    this.set(this.ORB_FILE_OUTPUT_DIR, fileOutputPath);
  }
  
/**
 * Return the fileInputPath
 */
  public String getFileInputPath() {
    return this.get(this.ORB_FILE_INPUT_DIR);
  }
  
/**
 * Set the fileInputPath
 * @param  String fileInputPath
 */
  public void setFileInputPath(String fileInputPath) {
    this.set(this.ORB_FILE_INPUT_DIR, fileInputPath);
  }
  
/**
 * Return the jobNumber
 */
  public String getJobNumber() {
    return this.get(this.ORB_JOB_NUMBER);
  }
  
/**
 * Set the jobNumber
 * @param  String jobNumber
 */
  public void setJobNumber(String jobNumber) {
    this.set(this.ORB_JOB_NUMBER, jobNumber);
  }
  
/**
 * Return the vertexClass
 */
  public Class<? extends Vertex> getVertexClass() {
    return (Class<? extends Vertex>) this.getClass(this.ORB_VERTEX_CLASS, Vertex.class);
  }
  
/**
 * Set the vertexClass
 * @param  Class<?> vertexClass
 */
  public void setVertexClass(Class<?> vertexClass) {
    this.set(this.ORB_VERTEX_CLASS, vertexClass.getCanonicalName());
  }
  
/**
 * Return the fileInputFormatClass
 */
  public Class<? extends InputFormat> getFileInputFormatClass() {
    return (Class<? extends InputFormat>) this.getClass(this.ORB_VERTEX_CLASS, InputFormat.class);
  }
  
/**
 * Set the fileInputFormatClass
 * @param  Class<?> fileInputFormatClass
 */
  public void setFileInputFormatClass(Class<?> fileInputFormatClass) {
    this.set(this.ORB_FILE_INPUT_FORMAT_CLASS, fileInputFormatClass.getCanonicalName());
  }
  
/**
 * Return the fileOutputFormatClass
 */
  public Class<? extends OutputFormat> getFileOutputFormatClass() {
    return (Class<? extends OutputFormat>) this.getClass(this.ORB_FILE_OUTPUT_FORMAT_CLASS,
      OutputFormat.class);
  }
  
/**
 * Set the fileOutputFormatClass
 * @param  Class<?> fileOutputFormatClass
 */
  public void setFileOutputFormatClass(Class<?> fileOutputFormatClass) {
    this.set(this.ORB_FILE_OUTPUT_FORMAT_CLASS, fileOutputFormatClass.getName());
  }
  
/**
 * Return the vertexInputFormatClass
 */
  public Class<? extends VertexBuilder> getVertexInputFormatClass() {
    return (Class<? extends VertexBuilder>) this.getClass(this.ORB_VERTEX_INPUT_FORMAT_CLASS,
      VertexBuilder.class);
  }
  
/**
 * Set the vertexInputFormatClass
 * @param  Class<?> vertexInputFormatClass
 */
  public void setVertexInputFormatClass(Class<?> vertexInputFormatClass) {
    this.set(this.ORB_VERTEX_INPUT_FORMAT_CLASS, vertexInputFormatClass.getName());
  }
  
/**
 * Return the numberOfPartitionsPerMachine
 */
  public int getNumberOfPartitionsPerMachine() {
    return Integer.parseInt(this.get(this.ORB_PARTITIONS_PER_MACHINE));
  }
  
/**
 * Set the numberOfPartitionsPerMachine
 * @param  int numberOfPartitionsPerMachine
 */
  public void setNumberOfPartitionsPerMachine(int numberOfPartitionsPerMachine) {
    this.set(this.ORB_PARTITIONS_PER_MACHINE, Integer.toString(numberOfPartitionsPerMachine));
  }
  
/**
 * Return the numberOfVertexThreads
 */
  public int getNumberOfVertexThreads() {
    return Integer.parseInt(this.get(this.ORB_PARTITION_VERTEX_THREADS));
  }
  
/**
 * Set the numberOfVertexThreads
 * @param  int numberOfVertexThreads
 */
  public void setNumberOfVertexThreads(int numberOfVertexThreads) {
    this.set(this.ORB_PARTITION_VERTEX_THREADS, Integer.toString(numberOfVertexThreads));
  }
  
/**
 * Return the numberOfMessageHandlers
 */
  public int getNumberOfMessageHandlers() {
    return Integer.parseInt(this.get(this.ORB_PARTITION_MESSAGEHANDLER_THREADS));
  }
  
/**
 * Set the numberOfMessageHandlers
 * @param  int i
 */
  public void setNumberOfMessageHandlers(int i) {
    this.set(this.ORB_PARTITION_MESSAGEHANDLER_THREADS, Integer.toString(i));
  }
  
/**
 * Return the orbClusterName
 */
  public String getOrbClusterName() {
    return new String(this.get(this.ORB_CLUSTER_NAME));
  }
  
/**
 * Set the orbClusterName
 * @param  String orbClusterName
 */
  public void setOrbClusterName(String orbClusterName) {
    this.set(this.ORB_CLUSTER_NAME, orbClusterName);
  }
  
/**
 * Return the orbJobName
 */
  public String getOrbJobName() {
    return new String(this.get(this.ORB_JOB_NAME));
  }
  
/**
 * Set the orbJobName
 * @param  String orbJobName
 */
  public void setOrbJobName(String orbJobName) {
    this.set(this.ORB_JOB_NAME, orbJobName);
  }
  
/**
 * Return the orbZooKeeperQuorum
 */
  public String getOrbZooKeeperQuorum() {
    return new String(this.get(this.ORB_ZOOKEEPER_QUORUM));
  }
  
/**
 * Set the orbZooKeeperQuorum
 * @param  String orbZooKeeperQuorum
 */
  public void setOrbZooKeeperQuorum(String orbZooKeeperQuorum) {
    this.set(this.ORB_ZOOKEEPER_QUORUM, orbZooKeeperQuorum);
  }
  
/**
 * Return the orbLauncherNetworkDevice
 */
  public String getOrbLauncherNetworkDevice() {
    return new String(this.get(this.ORB_LAUNCHER_NETWORKDEVICE));
  }
  
/**
 * Set the orbLauncherNetworkDevice
 * @param  String orbLauncherNetworkDevice
 */
  public void setOrbLauncherNetworkDevice(String orbLauncherNetworkDevice) {
    this.set(this.ORB_LAUNCHER_NETWORKDEVICE, orbLauncherNetworkDevice);
  }
  
/**
 * Return the orbPartitionJavaopts
 */
  public String getOrbPartitionJavaopts() {
    return new String(this.get(this.ORB_PARTITION_JAVAOPTS));
  }
  
/**
 * Set the orbPartitionJavaopts
 * @param  String orbPartitionJavaopts
 */
  public void setOrbPartitionJavaopts(String orbPartitionJavaopts) {
    this.set(this.ORB_PARTITION_JAVAOPTS, orbPartitionJavaopts);
  }
  
/**
 * Return the nameNode
 */
  public String getNameNode() {
    return new String(this.get(this.ORB_FS_DEFAULT_NAME));
  }
  
/**
 * Set the nameNode
 * @param  String orbFsDefaultName
 */
  public void setNameNode(String orbFsDefaultName) {
    this.set(this.ORB_FS_DEFAULT_NAME, orbFsDefaultName);
  }
  
/**
 * Return the orbBasePort
 */
  public int getOrbBasePort() {
    return Integer.parseInt(this.get(this.ORB_CLUSTER_BASEPORT));
  }
  
/**
 * Set the orbBasePort
 * @param  int orbBasePort
 */
  public void setOrbBasePort(int orbBasePort) {
    this.setInt(this.ORB_CLUSTER_BASEPORT, orbBasePort);
  }
  
/**
 * Set the orbClassPaths
 * @param  String[] orbClassPaths
 */
  public void setOrbClassPaths(String[] orbClassPaths) {
    this.setStrings(this.ORB_CLASS_PATHS, orbClassPaths);
  }
  
/**
 * Return the orbClassPaths
 */
  public String[] getOrbClassPaths() {
    return this.getStrings(this.ORB_CLASS_PATHS);
  }
  
/**
 * Set the orbClassPaths
 * @param  String string
 */
  public void setOrbClassPaths(String string) {
    this.set(this.ORB_CLASS_PATHS, string);
  }
  
/**
 * Return the networkInterface
 */
  public String getNetworkInterface() {
    return this.get(ORB_LAUNCHER_NETWORKDEVICE);
  }
  
/**
 * Return the orbTrackerPort
 */
  public int getOrbTrackerPort() {
    return Integer.parseInt(this.get(this.ORB_TRACKER_PORT));
  }
  
/**
 * Set the orbTrackerPort
 * @param  int trackerPort
 */
  public void setOrbTrackerPort(int trackerPort) {
    this.set(this.ORB_TRACKER_PORT, Integer.toString(trackerPort));
  }
  
/**
 * Return the jobHeartbeatTimeout
 */
  public long getJobHeartbeatTimeout() {
    return Integer.parseInt(this.get(this.ORB_JOB_HEARTBEAT_TIMEOUT));
  }
  
/**
 * Set the jobHeartbeatTimeout
 * @param  int heartbeatTimeout
 */
  public void setJobHeartbeatTimeout(int heartbeatTimeout) {
    this.setInt(this.ORB_JOB_HEARTBEAT_TIMEOUT, heartbeatTimeout);
  }
  
/**
 * Return the orbPartitionManagementBaseport
 */
  public int getOrbPartitionManagementBaseport() {
    return Integer.parseInt(this.get(this.ORB_PARTITION_MANAGEMENT_BASEPORT));
  }
  
/**
 * Set the orbPartitionManagementBaseport
 * @param  int port
 */
  public void setOrbPartitionManagementBaseport(int port) {
    this.setInt(this.ORB_PARTITION_MANAGEMENT_BASEPORT, port);
  }
  
/**
 * Return the orbRequestedPartitions
 */
  public int getOrbRequestedPartitions() {
    return Integer.parseInt(this.get(this.ORB_REQUESTED_PARTITIONS));
  }
  
/**
 * Set the orbRequestedPartitions
 * @param  int requested
 */
  public void setOrbRequestedPartitions(int requested) {
    this.setInt(this.ORB_REQUESTED_PARTITIONS, requested);
  }
  
/**
 * Return the orbReservedPartitions
 */
  public int getOrbReservedPartitions() {
    return Integer.parseInt(this.get(this.ORB_RESERVED_PARTITIONS));
  }
  
/**
 * Set the orbReservedPartitions
 * @param  int reserved
 */
  public void setOrbReservedPartitions(int reserved) {
    this.setInt(this.ORB_RESERVED_PARTITIONS, reserved);
  }
  
/**
 * Return the maximumJobTries
 */
  public int getMaximumJobTries() {
    return Integer.parseInt(this.get(this.ORB_JOB_MAX_TRIES));
  }
  
/**
 * Set the maximumJobTries
 * @param  int maxTries
 */
  public void setMaximumJobTries(int maxTries) {
    this.setInt(this.ORB_JOB_MAX_TRIES  , maxTries);
  }
  
/**
 * Set the verticesPerBlock
 * @param  int verticesPerBlock
 */
  public void setVerticesPerBlock(int verticesPerBlock) {
    this.setInt(this.ORB_NUMBER_VERTICES_BLOCK, verticesPerBlock);
  }
  
/**
 * Return the verticesPerBlock
 */
  public int getVerticesPerBlock() {
    return Integer.parseInt(this.get(this.ORB_NUMBER_VERTICES_BLOCK));
  }

/**
 * Set the messagesPerBlock
 * @param  int messagesPerBlock
 */
  public void setMessagesPerBlock(int messagesPerBlock) {
    this.setInt(this.ORB_NUMBER_MESSAGES_BLOCK, messagesPerBlock);
  }
  
/**
 * Return the messagesPerBlock
 */
  public int getMessagesPerBlock() {
    return Integer.parseInt(this.get(this.ORB_NUMBER_MESSAGES_BLOCK));
  }
  
/**
 * Set the handlersPerServer
 * @param  int handlersPerServer
 */
  public void setHandlersPerServer(int handlersPerServer) {
    this.setInt(this.ORB_HANDLERS_PER_RPC_SERVER, handlersPerServer);
  }
  
/**
 * Return the handlersPerServer
 */
  public int getHandlersPerServer() {
    return Integer.parseInt(this.get(this.ORB_HANDLERS_PER_RPC_SERVER));
  }

/**
 * Set the inputSplitHandlerThreads
 * @param  int inputSplitHandlerThreads
 */
  public void setInputSplitHandlerThreads(int inputSplitHandlerThreads) {
    this.setInt(this.ORB_INPUT_SPLIT_HANDLER_THREADS, inputSplitHandlerThreads);
  }
  
/**
 * Return the inputSplitHandlerThreads
 */
  public int getInputSplitHandlerThreads() {
    return Integer.parseInt(this.get(this.ORB_INPUT_SPLIT_HANDLER_THREADS));
  }

/**
 * Set the messageHandlerThreads
 * @param  int messageHandlerThreads
 */
  public void setMessageHandlerThreads(int messageHandlerThreads) {
    this.setInt(this.ORB_MESSAGE_HANDLER_THREADS, messageHandlerThreads);
  }
  
/**
 * Return the messageHandlerThreads
 */
  public int getMessageHandlerThreads() {
    return Integer.parseInt(this.get(this.ORB_MESSAGE_HANDLER_THREADS));
  }

/**
 * Set the computeThreads
 * @param  int computeThreads
 */
  public void setComputeThreads(int computeThreads) {
    this.setInt(this.ORB_COMPUTE_THREADS, computeThreads);
  }
  
/**
 * Return the computeThreads
 */
  public int getComputeThreads() {
    return Integer.parseInt(this.get(this.ORB_COMPUTE_THREADS));
  }

/**
 * Return the verticesLoaderHandlerThreads
 */
  public int getVerticesLoaderHandlerThreads() {
    return Integer.parseInt(this.get(this.ORB_VERTICES_HANDLER_THREADS));
  }
  
/**
 * Set the verticesLoaderHandlerThreads
 * @param  int vertexThreads
 */
  public void setVerticesLoaderHandlerThreads(int vertexThreads) {
    this.setInt(this.ORB_VERTICES_HANDLER_THREADS, vertexThreads);
  }

}
