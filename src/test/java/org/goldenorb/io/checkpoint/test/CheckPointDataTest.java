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
package org.goldenorb.io.checkpoint.test;

import org.goldenorb.io.output.checkpoint.CheckPointDataOutput;
import org.goldenorb.io.input.checkpoint.CheckPointDataInput;
import org.goldenorb.conf.OrbConfiguration;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import org.junit.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * This class provides testing for the checkpointing mechanism, both input and output.
 */
public class CheckPointDataTest {
  
  private static MiniDFSCluster cluster;
  private static FileSystem fs;
  
  /**
   * Set up the MiniDFSCluster.
   */
  @BeforeClass
  public static void setUpCluster() throws Exception {
    Configuration conf = new Configuration(true);
    cluster = new MiniDFSCluster(conf, 1, true, null);
    fs = cluster.getFileSystem();
  }
  
  /**
   * Tests the CheckPointDataOutput class by writing several different types of Writables to the checkpoint.
   * 
   * @throws Exception
   */
  @Test
  public void testCheckpointOutput() throws Exception {
    
    int superStep = 0;
    int partition = 0;
    OrbConfiguration orbConf = new OrbConfiguration();
    orbConf.set("fs.default.name", "hdfs://localhost:" + cluster.getNameNodePort());
    orbConf.setJobNumber("0");
    orbConf.setFileOutputPath("test");
    
    CheckPointDataOutput checkpointOutput = new CheckPointDataOutput(orbConf, superStep, partition);
    
    IntWritable intOutput = new IntWritable(4);
    intOutput.write(checkpointOutput);
    
    LongWritable longOutput = new LongWritable(9223372036854775807L);
    longOutput.write(checkpointOutput);
    
    Text textOutput = new Text("test");
    textOutput.write(checkpointOutput);
    
    FloatWritable floatOutput = new FloatWritable(3.14159F);
    floatOutput.write(checkpointOutput);
    
    checkpointOutput.close();
    
    assertThat(checkpointOutput, notNullValue());
  }
  
  /**
   * Tests the CheckPointDataInput class by reading several different types of Writables from the checkpoint.
   * Asserts that Writables that were written in are of the same value and type when reading in from HDFS.
   * 
   * @throws Exception
   */
  @Test
  public void testCheckpointInput() throws Exception {
    
    int superStep = 0;
    int partition = 0;
    OrbConfiguration orbConf = new OrbConfiguration();
    orbConf.set("fs.default.name", "hdfs://localhost:" + cluster.getNameNodePort());
    orbConf.setJobNumber("0");
    orbConf.setFileOutputPath("test");
    
    CheckPointDataInput checkpointInput = new CheckPointDataInput(orbConf, superStep, partition);
    
    // Data is read on a FIFO basis
    
    IntWritable intInput = new IntWritable();
    intInput.readFields(checkpointInput);
    
    LongWritable longInput = new LongWritable();
    longInput.readFields(checkpointInput);
    
    Text textInput = new Text();
    textInput.readFields(checkpointInput);
    
    FloatWritable floatInput = new FloatWritable();
    floatInput.readFields(checkpointInput);
    
    checkpointInput.close();
    
    assertThat(checkpointInput, notNullValue());
    assertEquals(intInput.get(), 4);
    assertEquals(longInput.get(), 9223372036854775807L);
    assertEquals(textInput.toString(), "test");
    assertTrue(floatInput.get() == 3.14159F);
  }
  
  /**
   * Shuts down MiniDFSCluster and closes the associated FileSystem.
   */
  @AfterClass
  public static void tearDownCluster() throws IOException {
    if (fs != null) {
      fs.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
}