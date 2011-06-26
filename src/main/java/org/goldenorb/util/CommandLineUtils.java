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
package org.goldenorb.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;

/**
 * This class provides a command line interface utility class for GoldenOrb.
 * 
 */
public class CommandLineUtils {
  
  private static SortedMap<String,String> helpMap;
  private static SortedMap<String,SortedMap<String,String>> validArguments;
  
  /**
   * Main method for the CLI.
   * 
   * @param args
   */
  public static void main(String[] args) {
    
    if (args.length == 0) {
      System.out.println("ERROR : No command received. Use command 'Help' for a list of commands.");
      System.exit(-1);
    }
    if (args[0].equalsIgnoreCase("Help")) {
      help(args);
    } else if (args[0].equalsIgnoreCase("Job")) {
      if (args.length >= 1) job(args);
      else {
        System.out.println("Job command requires arguments.");
      }
    } else { // Invalid Command
      System.out.println("Error : Invalid Command. Command '" + args[0] + "' not recognized.");
      System.exit(-1);
    }
    
  }
  
  /**
   * Provides a "help" command in the CLI.
   * 
   * @param args
   *          - String[]
   */
  public static void help(String[] args) {
    initializeHelpMaps();
    Set<String> commandsSet = helpMap.keySet();
    if (args.length == 1) {
      for (String command : commandsSet) {
        System.out.print(command + "\t" + helpMap.get(command));
        if (!command.equalsIgnoreCase("Help")) {
          System.out.println(" Use 'Help -" + command + "' to view a list of valid arguments");
        }
      }
    } else { // option provided
      String command = args[1].replaceFirst("-", "");
      if (commandsSet.contains(command)) {
        System.out.println("Help for command '" + command + "'.");
        System.out.println(command + "\t" + helpMap.get(command));
        System.out.println("List of valid arguments for command '" + command + "' : ");
        SortedMap<String,String> argMap = validArguments.get(command);
        Set<String> arguments = argMap.keySet();
        for (String arg : arguments) {
          System.out.println(command + "\t" + argMap.get(arg));
        }
      } else {
        System.out.println(args[0] + " is not a valid command.");
      }
    }
  }
  
  /**
   * Initializes the helpMaps used for the CLI.
   */
  public static void initializeHelpMaps() {
    helpMap = new TreeMap<String,String>();
    // Job
    helpMap.put("Job", "Lets you kill jobs and view job status.");
    // Help
    helpMap.put("Help", "Displays all of the commands and their general purpose.");
    
    validArguments = new TreeMap<String,SortedMap<String,String>>();
    // Job
    validArguments.put("Job", jobHelpValidArguments());
  }
  
  /**
   * Defines valid arguments.
   * 
   * @returns SortedMap<String,String>
   */
  public static SortedMap<String,String> jobHelpValidArguments() {
    SortedMap<String,String> map = new TreeMap<String,String>();
    // kill
    map.put("-kill", "Terminates the job whose JobID is supplied.  ex. Job -kill ClusterName Job1234 ,\n\t\t"
                     + "would kill the job whose JobID is '1234' on cluster 'ClusterName.");
    // status
    map.put(
      "-status",
      "If supplied a JobID and cluster, it will display the job status of that specific job.\n\t\t"
          + "If no JobID is provided, but a cluster name is provied, it will list the status of all jobs on that cluster.\n\t\t"
          + "ex. Job -status ClusterName Job1234 , would display the status of 'Job1234' on cluster 'ClusterName'.");
    return map;
  }
  
  /**
   * Provides a "job" command in the CLI.
   * 
   * @param args
   *          - String[]
   */
  public static void job(String[] args) {
    ZooKeeper zk = connectZookeeper();
    if (args[1].equalsIgnoreCase("-kill")) {
      jobKill(args, zk);
    } else if (args[1].equalsIgnoreCase("-status")) {
      jobStatus(args, zk);
    } else {
      System.out
          .println(args[1]
                   + "is an invalid argument for 'Job'. Use 'Help -Job' for a list of valid arguments.");
    }
  }
  
  /**
   * Kills a Job.
   * 
   * @param args
   *          - String[]
   * @param zk
   *          - ZooKeeper
   */
  public static void jobKill(String[] args, ZooKeeper zk) {
    if (args.length < 3) {
      System.out.println("'Job -kill' requires a cluster name and Job ID!");
    } else if (args.length == 3) {
      System.out.println("'Job -kill' requires a Job ID!");
    } else if (ZookeeperUtils.nodeExists(zk, "/GoldenOrb/" + args[2])) { // cluster exists
      if (ZookeeperUtils.nodeExists(zk, "/GoldenOrb/" + args[2] + "/JobQueue/" + args[3])) { // job is in
                                                                                             // queue
        try {
          ZookeeperUtils.deleteNodeIfEmpty(zk, "/GoldenOrb/" + args[2] + "/JobQueue/" + args[3]);
          System.out.println("Killed " + args[2] + " on cluster " + args[3]);
        } catch (OrbZKFailure e) {
          System.err.println("Error occured while trying delete " + args[3] + " from the JobQueue.");
          e.printStackTrace();
          System.exit(-1);
        }
      } else if (ZookeeperUtils.nodeExists(zk, "/GoldenOrb/" + args[2] + "/JobsInProgress/" + args[3])) { // job
                                                                                                          // is
                                                                                                          // running
        try {
          ZookeeperUtils.tryToCreateNode(zk, "/GoldenOrb/" + args[2] + "/JobsInProgress/" + args[3]
                                             + "/messages/death");
          System.out.println("Killed " + args[2] + " on cluster " + args[3]);
        } catch (OrbZKFailure e) {
          System.err.println("Error occured while trying kill " + args[3] + " on cluster " + args[2]);
          e.printStackTrace();
          System.exit(-1);
        }
      } else { // Job does not exist
        System.out.println("Job " + args[3] + " on cluster " + args[2] + " does not exist.");
      }
    } else {
      System.out.println("Cluster " + args[2] + " does not exist.");
    }
  }
  
  /**
   * Checks the status of a Job.
   * 
   * @param args
   *          - String[]
   * @param zk
   *          - ZooKeeper
   */
  public static void jobStatus(String[] args, ZooKeeper zk) {
    if (args.length < 3) {
      System.out.println("No cluster name provided.");
    } else if (!ZookeeperUtils.nodeExists(zk, "/GoldenOrb/" + args[2])) {
      System.out.println("Cluster " + args[2] + " does not exist.");
    } else if (args.length == 3) { // display status of all jobs on cluster
      try {
        List<String> jobsInQueue = zk.getChildren("/GoldenOrb/" + args[2] + "/JobQueue", false);
        List<String> jobsInProgress = zk.getChildren("/GoldenOrb/" + args[2] + "/JobsInProgress", false);
        System.out.println(jobsInQueue.size() + " jobs in JobQueue :");
        Collections.sort(jobsInQueue);
        for (String job : jobsInQueue) {
          System.out.println("\t" + job);
        }
        System.out.println("\n" + jobsInProgress.size() + " jobs in progress :");
        for (String job : jobsInProgress) {
          System.out.println("\t" + job);
        }
      } catch (Exception e) {
        System.err.println("Error occured while attempting to get status of jobs on cluster " + args[2]);
        e.printStackTrace();
        System.exit(-1);
      }
    } else { // JobID is provided
      if (ZookeeperUtils.nodeExists(zk, "/GoldenOrb/" + args[2] + "/JobQueue/" + args[3])) {
        try {
          List<String> children = zk.getChildren("/GoldenOrb/" + args[2] + "/JobQueue", false);
          Collections.sort(children);
          int place = children.indexOf(args[3]) + 1;
          String placeInQueue;
          if (place == 1) {
            placeInQueue = "1st";
          } else if (place == 2) {
            placeInQueue = "2nd";
          } else if (place == 3) {
            placeInQueue = "3rd";
          } else {
            placeInQueue = place + "th";
          }
          System.out.println(args[3] + " is " + placeInQueue + " in the JobQueue.");
        } catch (Exception e) {
          System.err.println("Error occurred while looking up job status : ");
          e.printStackTrace();
        }
      } else if (ZookeeperUtils.nodeExists(zk, "/GoldenOrb/" + args[2] + "/JobsInProgress/" + args[3])) {
        System.out.println(args[3] + " is in progress.");
      } else {
        System.out.println("Job " + args[3] + " does not exist on cluster " + args[2] + ".");
      }
    }
  }
  
  /**
   * Connects to ZooKeeper.
   * 
   * @returns ZooKeeper
   */
  public static ZooKeeper connectZookeeper() {
    OrbConfiguration orbConf = new OrbConfiguration(true);
    ZooKeeper zk = null;
    try {
      zk = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
    } catch (IOException e) {
      System.err.println("Could not connect : ");
      e.printStackTrace();
      System.exit(-1);
    } catch (InterruptedException e) {
      System.err.println("Could not connect : ");
      e.printStackTrace();
      System.exit(-1);
    }
    return zk;
  }
}
