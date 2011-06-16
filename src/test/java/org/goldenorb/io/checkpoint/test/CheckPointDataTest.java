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

public class CheckPointDataTest {
  
  private static MiniDFSCluster cluster;
  private static FileSystem fs;
  
  @BeforeClass
  public static void setUpCluster() throws Exception {
    Configuration conf = new Configuration(true);
    cluster = new MiniDFSCluster(conf, 1, true, null);
    fs = cluster.getFileSystem();
  }
  
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