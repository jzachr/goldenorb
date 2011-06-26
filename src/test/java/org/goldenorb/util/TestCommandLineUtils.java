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
package org.goldenorb.util;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestCommandLineUtils {
  
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final static String clusterName = "Test_CLU";
  private static ZooKeeper zk;
  private String baseJIP = "/GoldenOrb/"+clusterName+"/JobsInProgress";
  private String baseJIQ = "/GoldenOrb"+clusterName+"/JobQueue";
  
/**
 * Set the upZooKeeperNodes
 */
  @BeforeClass
  public static void setupZooKeeperNodes() throws OrbZKFailure, IOException, InterruptedException {
    zk = ZookeeperUtils.connect("localhost");
    ZookeeperUtils.tryToCreateNode(zk, "/GoldenOrb");
    ZookeeperUtils.tryToCreateNode(zk, "/GoldenOrb/"+clusterName);
    ZookeeperUtils.tryToCreateNode(zk, "/GoldenOrb/"+clusterName+"/JobQueue");
    ZookeeperUtils.tryToCreateNode(zk, "/GoldenOrb/"+clusterName+"/JobsInProgress");
    addNodeToJobsInProgress("Job1");
    addNodeToJobsInProgress("Job2");
    addNodeToJobsInProgress("Job3");
    addNodeToJobQueue("Job4");
    addNodeToJobQueue("Job5");
    addNodeToJobQueue("Job6");
  }
  
/**
 * 
 */
  @AfterClass
  public static void tearDownTestNodes() throws OrbZKFailure {
    ZookeeperUtils.recursiveDelete(zk, "/GoldenOrb/"+clusterName);
    ZookeeperUtils.deleteNodeIfEmpty(zk, "/GoldenOrb/"+clusterName);
  }

/**
 * Set the upStreams
 */
  @Before
  public void setUpStreams() {
      System.setOut(new PrintStream(outContent));
  }

/**
 * 
 */
  @After
  public void cleanUpStreams() {
      System.setOut(null);
  }

  
  /**
   * Test method for {@link org.goldenorb.util.CommandLineUtils#main(java.lang.String[])}.
 * @throws OrbZKFailure 
   */
 // @Test
  public void testMain() throws OrbZKFailure {
	  String[] args = {"Job", "-kill", "Test_CLU", "Job5"};
	  CommandLineUtils.job(args);
	  assertTrue(!ZookeeperUtils.nodeExists(zk, baseJIQ+"/Job5"));
	  addNodeToJobQueue("Job5");
  }
  
  
  /**
   * Test method for {@link org.goldenorb.util.CommandLineUtils#help(java.lang.String[])}.
   */
  @Test
  public void testHelp() {
    String[] args1 = {"Help", "-Job"};
    String[] args2 = {"Help"};
    CommandLineUtils.help(args1);
    String out1 = outContent.toString();
    System.setOut(null);
    System.setOut(new PrintStream(outContent));
    CommandLineUtils.help(args2);
    String out2 = outContent.toString();
    assertTrue(!out1.equals(""));
    assertTrue(!out2.equals(""));
    assertTrue(out1 != out2);
  }
  
  
  /**
   * Test method for {@link org.goldenorb.util.CommandLineUtils#job(java.lang.String[])}.
   */
  @Test
  public void testJob1() {
	  String[] args = {"Job", "-kill", "Test_CLU", "Job2"}; // Job1 is set up as in progress
	  CommandLineUtils.job(args);
	  assertTrue(ZookeeperUtils.nodeExists(zk, baseJIP+"/Job2/messages/death"));
  }
  @Test
  public void testJob2() throws OrbZKFailure {
	  String[] args = {"Job", "-kill", "Test_CLU", "Job5"};
	  CommandLineUtils.job(args);
	  assertTrue(!ZookeeperUtils.nodeExists(zk, baseJIQ+"/Job5"));
	  addNodeToJobQueue("Job5");
  }
  @Test
  public void testJob3() {
	  String[] args = {"Job", "-status", "Test_CLU", "Job3"};
	  CommandLineUtils.job(args);
	  assertTrue(!ZookeeperUtils.nodeExists(zk, baseJIP+"/Job3/messages/death"));
	  assertTrue(ZookeeperUtils.nodeExists(zk, baseJIP+"/Job3"));
	  assertTrue(outContent.toString().equalsIgnoreCase(args[3]+" is in progress.\n"));
  }
  
  
  /**
   * Test method for {@link org.goldenorb.util.CommandLineUtils#jobKill(java.lang.String[], org.apache.zookeeper.ZooKeeper)}.
   * @throws InterruptedException 
   * @throws IOException 
   * @throws OrbZKFailure 
   */
  @Test
  public void testJobKill1() throws IOException, InterruptedException, OrbZKFailure {
    String[] args1 = {"Job", "-kill", "Test_CLU", "Job1"}; // Job1 is set up as in progress
    CommandLineUtils.jobKill(args1, zk);
    assertTrue(ZookeeperUtils.nodeExists(zk, baseJIP+"/Job1/messages/death"));
  }
  @Test
  public void testJobKill2() throws OrbZKFailure {
    String[] args2 = {"Job", "-kill", "Test_CLU", "Job4"}; // Job4 is set up in JobQueue
    CommandLineUtils.jobKill(args2, zk);
    assertTrue(!ZookeeperUtils.nodeExists(zk, baseJIQ+"/Job4"));
    addNodeToJobQueue("Job4");
  }
   
  /**
   * Test method for {@link org.goldenorb.util.CommandLineUtils#jobStatus(java.lang.String[], org.apache.zookeeper.ZooKeeper)}.
   */
  @Test
  public void testJobStatus1() {
    String[] args = {"Job", "-status", "Test_CLU", "Job1"};
    CommandLineUtils.jobStatus(args, zk);
    System.err.println(outContent.toString());
    assertTrue(outContent.toString().equals(args[3]+" is in progress.\n"));
  }
  @Test
  public void testJobStatus2() {
	    String[] args = {"Job", "-status", "Test_CLU", "Job6"};
	    CommandLineUtils.jobStatus(args, zk);
	    System.err.println(outContent.toString());
	    assertTrue(outContent.toString().equals("Job6 is 3rd in the JobQueue.\n"));
	  }
  @Test
  public void testJobStatus3() {
	    String[] args = {"Job", "-status", "Test", "Job1"};
	    CommandLineUtils.jobStatus(args, zk);
	    System.err.println(outContent.toString());
	    assertTrue(outContent.toString().equals("Cluster Test does not exist.\n"));
  }
  @Test
  public void testJobStatus4() {
	    String[] args = {"Job", "-status", "Test_CLU", "Job9"};
	    CommandLineUtils.jobStatus(args, zk);
	    System.err.println(outContent.toString());
	    assertTrue(outContent.toString().equals("Job "+args[3]+ " does not exist on cluster "+args[2]+".\n"));
  }
  
/**
 * 
 * @param  String nodeName
 */
  private static void addNodeToJobsInProgress(String nodeName) throws OrbZKFailure {
    String basePath = "/GoldenOrb/"+clusterName+"/JobsInProgress";
    ZookeeperUtils.tryToCreateNode(zk, basePath+"/"+nodeName);
    ZookeeperUtils.tryToCreateNode(zk, basePath+"/"+nodeName+"/messages");
  }
  
/**
 * 
 * @param  String nodeName
 */
  private static void addNodeToJobQueue(String nodeName) throws OrbZKFailure {
    ZookeeperUtils.tryToCreateNode(zk, "/GoldenOrb/"+clusterName+"/JobQueue/"+nodeName);
  }
  
}
