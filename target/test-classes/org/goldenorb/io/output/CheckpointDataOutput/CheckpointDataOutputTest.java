package org.goldenorb.io.output.CheckpointDataOutput;

import static org.junit.Assert.*;


import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.io.output.checkpoint.CheckpointDataOutput;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CheckpointDataOutputTest {
  
  private MiniDFSCluster cluster;
  private FileSystem fs;
  private static final OrbConfiguration ORBCONF = new OrbConfiguration();
  private static final int DFS_REPLICATION_INTERVAL = 1;
 

//  static {
//
//    ORBCONF.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 100);
//
//    ORBCONF.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 1);
//
//    ORBCONF.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
//
//    DFS_REPLICATION_INTERVAL);
//
//    ORBCONF.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY,
//
//    DFS_REPLICATION_INTERVAL);
//
//    }
  /*
  @Before
  public void setUp() throws Exception {
    //ORBCONF.setNameNode("localhost:18833");
    //Configuration conf = new Configuration();
    if (System.getProperty("test.build.data") == null) {
      System.setProperty("test.build.data", "/tmp");
    }
    cluster = new MiniDFSCluster(ORBCONF, 1, true, null);
//    cluster = new MiniDFSCluster();
    
    fs = cluster.getFileSystem();
    
  }
  
  @After
  public void tearDown() throws Exception {
    if (fs != null) { fs.close(); }
    if (cluster != null) {cluster.shutdown();}
  }
  
  @Test
  public void testCheckpointDataOutput() throws IOException {
    ORBCONF.setFileOutputPath("destinationDirectory");
    ORBCONF.setJobNumber("72704");
    CheckpointDataOutput dataOut = new CheckpointDataOutput(ORBCONF, 12345, 234);
    assertTrue(dataOut != null);
  }
  
  @Test
  public void testWriteInt() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testWriteByteArray() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testWriteByteArrayIntInt() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testWriteBoolean() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testWriteByte() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testWriteShort() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testWriteChar() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testWriteInt1() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testWriteLong() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testWriteFloat() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testWriteDouble() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testWriteBytes() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testWriteChars() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testWriteUTF() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testClose() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testSetOrbConf() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testGetOrbConf() {
    fail("Not yet implemented");
  }
  */
}
