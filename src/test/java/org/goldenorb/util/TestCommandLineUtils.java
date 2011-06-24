/**
 * 
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

/**
 * @author rebanks
 *
 */
public class TestCommandLineUtils {
  
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final static String clusterName = "Test_CLU";
  private static ZooKeeper zk;
  private String baseJIP = "/GoldenOrb/"+clusterName+"/JobsInProgress";
  private String baseJIQ = "/GoldenOrb"+clusterName+"/JobQueue";
  
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
  
  @AfterClass
  public static void tearDownTestNodes() throws OrbZKFailure {
    ZookeeperUtils.recursiveDelete(zk, "/GoldenOrb/"+clusterName);
    ZookeeperUtils.deleteNodeIfEmpty(zk, "/GoldenOrb/"+clusterName);
  }

  @Before
  public void setUpStreams() {
      System.setOut(new PrintStream(outContent));
  }

  @After
  public void cleanUpStreams() {
      System.setOut(null);
  }

  
  /**
   * Test method for {@link org.goldenorb.util.CommandLineUtils#main(java.lang.String[])}.
   */
 // @Test
  public void testMain() {
    fail("Not yet implemented");
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
 // @Test
  public void testJob() {
    fail("Not yet implemented");
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
    assertTrue(ZookeeperUtils.nodeExists(zk, baseJIP+"/Job1/messages/Kill"));
  }
  @Test
  public void testJobKill2() {
    String[] args2 = {"Job", "-kill", "Test_CLU", "Job4"}; // Job4 is set up in JobQueue
    CommandLineUtils.jobKill(args2, zk);
    assertTrue(!ZookeeperUtils.nodeExists(zk, baseJIQ+"/Job4"));
  }
   
  /**
   * Test method for {@link org.goldenorb.util.CommandLineUtils#jobStatus(java.lang.String[], org.apache.zookeeper.ZooKeeper)}.
   */
 // @Test
  public void testJobStatus1() {
    fail("Not yet implemented");
  }
  
  private static void addNodeToJobsInProgress(String nodeName) throws OrbZKFailure {
    String basePath = "/GoldenOrb/"+clusterName+"/JobsInProgress";
    ZookeeperUtils.tryToCreateNode(zk, basePath+"/"+nodeName);
    ZookeeperUtils.tryToCreateNode(zk, basePath+"/"+nodeName+"/messages");
  }
  
  private static void addNodeToJobQueue(String nodeName) throws OrbZKFailure {
    ZookeeperUtils.tryToCreateNode(zk, "/GoldenOrb/"+clusterName+"/JobQueue/"+nodeName);
  }
  
}
