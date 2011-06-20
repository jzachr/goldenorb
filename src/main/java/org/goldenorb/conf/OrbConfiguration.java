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
  
  public static final String ORB_ERROR_OUTPUT_STREAM = "goldenOrb.error.output.stream";
  public static final String ORB_SYSTEM_OUTPUT_STREAM = "goldenOrb.system.output.stream";
  
  public static final String ORB_FS_DEFAULT_NAME = "fs.default.name";
  public static final String ORB_FILE_INPUT_FORMAT_CLASS = "mapreduce.inputformat.class";
  public static final String ORB_FILE_OUTPUT_FORMAT_CLASS = "mapreduce.outputformat.class";
  public static final String ORB_FILE_INPUT_DIR = "mapred.input.dir";
  public static final String ORB_FILE_OUTPUT_DIR = "mapred.output.dir";
  
  public OrbConfiguration() {}
  
  public OrbConfiguration(boolean loadDefaults) {
    
    super(loadDefaults);
    if (loadDefaults) this.addOrbResources((Configuration) this);
    else {
      // need the file to load if not defaults
    }
  }
  
  private static Configuration addOrbResources(Configuration conf) {
    conf.addDefaultResource("orb-default.xml");
    conf.addDefaultResource("orb-site.xml");
    return conf;
  }
  
  @Override
  public boolean equals(Object rhs) {
    return this.getJobNumber().equals(((OrbConfiguration) rhs).getJobNumber());
  }

  public Class<?> getMessageClass() throws ClassNotFoundException {
    return Class.forName(this.get(this.ORB_MESSAGE_CLASS));
  }
  
  public void setMessageClass(Class<?> messageClass) {
    this.set(this.ORB_MESSAGE_CLASS, messageClass.getCanonicalName());
  }
  
  public Class<? extends VertexWriter> getVertexOutputFormatClass() {
    return (Class<? extends VertexWriter>) this.getClass(this.ORB_VERTEX_OUTPUT_FORMAT_CLASS,
      VertexWriter.class);
  }
  
  public void setVertexOutputFormatClass(Class<?> vertexOutputFormatClass) {
    this.set(this.ORB_VERTEX_OUTPUT_FORMAT_CLASS, vertexOutputFormatClass.getCanonicalName());
  }
  
  public String getFileOutputPath() {
    return this.get(this.ORB_FILE_OUTPUT_DIR);
  }
  
  public void setFileOutputPath(String fileOutputPath) {
    this.set(this.ORB_FILE_OUTPUT_DIR, fileOutputPath);
  }
  
  public String getFileInputPath() {
    return this.get(this.ORB_FILE_INPUT_DIR);
  }
  
  public void setFileInputPath(String fileInputPath) {
    this.set(this.ORB_FILE_INPUT_DIR, fileInputPath);
  }
  
  public String getJobNumber() {
    return this.get(this.ORB_JOB_NUMBER);
  }
  
  public void setJobNumber(String jobNumber) {
    this.set(this.ORB_JOB_NUMBER, jobNumber);
  }
  
  public Class<? extends Vertex> getVertexClass() {
    return (Class<? extends Vertex>) this.getClass(this.ORB_VERTEX_CLASS, Vertex.class);
  }
  
  public void setVertexClass(Class<?> vertexClass) {
    this.set(this.ORB_VERTEX_CLASS, vertexClass.getCanonicalName());
  }
  
  public Class<? extends InputFormat> getFileInputFormatClass() {
    return (Class<? extends InputFormat>) this.getClass(this.ORB_VERTEX_CLASS, InputFormat.class);
  }
  
  public void setFileInputFormatClass(Class<?> fileInputFormatClass) {
    this.set(this.ORB_FILE_INPUT_FORMAT_CLASS, fileInputFormatClass.getCanonicalName());
  }
  
  public Class<? extends OutputFormat> getFileOutputFormatClass() {
    return (Class<? extends OutputFormat>) this.getClass(this.ORB_FILE_OUTPUT_FORMAT_CLASS,
      OutputFormat.class);
  }
  
  public void setFileOutputFormatClass(Class<?> fileOutputFormatClass) {
    this.set(this.ORB_FILE_OUTPUT_FORMAT_CLASS, fileOutputFormatClass.getName());
  }
  
  public Class<? extends VertexBuilder> getVertexInputFormatClass() {
    return (Class<? extends VertexBuilder>) this.getClass(this.ORB_VERTEX_INPUT_FORMAT_CLASS,
      VertexBuilder.class);
  }
  
  public void setVertexInputFormatClass(Class<?> vertexInputFormatClass) {
    this.set(this.ORB_VERTEX_INPUT_FORMAT_CLASS, vertexInputFormatClass.getName());
  }
  
  public int getNumberOfPartitionsPerMachine() {
    return Integer.parseInt(this.get(this.ORB_PARTITIONS_PER_MACHINE));
  }
  
  public void setNumberOfPartitionsPerMachine(int numberOfPartitionsPerMachine) {
    this.set(this.ORB_PARTITIONS_PER_MACHINE, Integer.toString(numberOfPartitionsPerMachine));
  }
  
  public int getNumberOfVertexThreads() {
    return Integer.parseInt(this.get(this.ORB_PARTITION_VERTEX_THREADS));
  }
  
  public void setNumberOfVertexThreads(int numberOfVertexThreads) {
    this.set(this.ORB_PARTITION_VERTEX_THREADS, Integer.toString(numberOfVertexThreads));
  }
  
  public int getNumberOfMessageHandlers() {
    return Integer.parseInt(this.get(this.ORB_PARTITION_MESSAGEHANDLER_THREADS));
  }
  
  public void setNumberOfMessageHandlers(int i) {
    this.set(this.ORB_PARTITION_MESSAGEHANDLER_THREADS, Integer.toString(i));
  }
  
  public String getOrbClusterName() {
    return new String(this.get(this.ORB_CLUSTER_NAME));
  }
  
  public void setOrbClusterName(String orbClusterName) {
    this.set(this.ORB_CLUSTER_NAME, orbClusterName);
  }
  
  public String getOrbJobName() {
    return new String(this.get(this.ORB_JOB_NAME));
  }
  
  public void setOrbJobName(String orbJobName) {
    this.set(this.ORB_JOB_NAME, orbJobName);
  }
  
  public String getOrbZooKeeperQuorum() {
    return new String(this.get(this.ORB_ZOOKEEPER_QUORUM));
  }
  
  public void setOrbZooKeeperQuorum(String orbZooKeeperQuorum) {
    this.set(this.ORB_ZOOKEEPER_QUORUM, orbZooKeeperQuorum);
  }
  
  public String getOrbLauncherNetworkDevice() {
    return new String(this.get(this.ORB_LAUNCHER_NETWORKDEVICE));
  }
  
  public void setOrbLauncherNetworkDevice(String orbLauncherNetworkDevice) {
    this.set(this.ORB_LAUNCHER_NETWORKDEVICE, orbLauncherNetworkDevice);
  }
  
  public String getOrbPartitionJavaopts() {
    return new String(this.get(this.ORB_PARTITION_JAVAOPTS));
  }
  
  public void setOrbPartitionJavaopts(String orbPartitionJavaopts) {
    this.set(this.ORB_PARTITION_JAVAOPTS, orbPartitionJavaopts);
  }
  
  public String getNameNode() {
    return new String(this.get(this.ORB_FS_DEFAULT_NAME));
  }
  
  public void setNameNode(String orbFsDefaultName) {
    this.set(this.ORB_FS_DEFAULT_NAME, orbFsDefaultName);
  }
  
  public int getOrbBasePort() {
    return Integer.parseInt(this.get(this.ORB_CLUSTER_BASEPORT));
  }
  
  public void setOrbBasePort(int orbBasePort) {
    this.setInt(this.ORB_CLUSTER_BASEPORT, orbBasePort);
  }
  
  public void setOrbClassPaths(String[] orbClassPaths) {
    this.setStrings(this.ORB_CLASS_PATHS, orbClassPaths);
  }
  
  public String[] getOrbClassPaths() {
    return this.getStrings(this.ORB_CLASS_PATHS);
  }
  
  public void setOrbClassPaths(String string) {
    this.set(this.ORB_CLASS_PATHS, string);
  }
  
  public String getNetworkInterface() {
    return this.get(ORB_LAUNCHER_NETWORKDEVICE);
  }
  
  public int getOrbTrackerPort() {
    return Integer.parseInt(this.get(this.ORB_TRACKER_PORT));
  }
  
  public void setOrbTrackerPort(int trackerPort) {
    this.set(this.ORB_TRACKER_PORT, Integer.toString(trackerPort));
  }
  
  public long getJobHeartbeatTimeout() {
    return Integer.parseInt(this.get(this.ORB_JOB_HEARTBEAT_TIMEOUT));
  }
  
  public int getOrbPartitionManagementBaseport() {
    return Integer.parseInt(this.get(this.ORB_PARTITION_MANAGEMENT_BASEPORT));
  }
  
  public void setOrbPartitionManagementBaseport(int port) {
    this.setInt(this.ORB_PARTITION_MANAGEMENT_BASEPORT, port);
  }
  
  public int getOrbRequestedPartitions() {
    return Integer.parseInt(this.get(this.ORB_REQUESTED_PARTITIONS));
  }
  
  public void setOrbRequestedPartitions(int requested) {
    this.setInt(this.ORB_REQUESTED_PARTITIONS, requested);
  }
  
  public int getOrbReservedPartitions() {
    return Integer.parseInt(this.get(this.ORB_RESERVED_PARTITIONS));
  }
  
  public void setOrbReservedPartitions(int reserved) {
    this.setInt(this.ORB_RESERVED_PARTITIONS, reserved);
  }
}
