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
package org.goldenorb.io.input;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.util.ReflectionUtils;
import org.goldenorb.conf.OrbConfigurable;
import org.goldenorb.conf.OrbConfiguration;

@SuppressWarnings("deprecation")
public class VertexInput<INPUT_KEY,INPUT_VALUE> implements OrbConfigurable {
  
  private OrbConfiguration orbConf;
  private BytesWritable rawSplit;
  private String splitClass;
  private int partitionID;
  private RecordReader<INPUT_KEY,INPUT_VALUE> recordReader;
  
  /**
   * Constructor
   * 
   */
  public VertexInput() {}
  
  /**
 * 
 */
  @SuppressWarnings("unchecked")
  public void initialize() {
    // rebuild the input split
    org.apache.hadoop.mapreduce.InputSplit split = null;
    DataInputBuffer splitBuffer = new DataInputBuffer();
    splitBuffer.reset(rawSplit.getBytes(), 0, rawSplit.getLength());
    SerializationFactory factory = new SerializationFactory(orbConf);
    Deserializer<? extends org.apache.hadoop.mapreduce.InputSplit> deserializer;
    try {
      deserializer = (Deserializer<? extends org.apache.hadoop.mapreduce.InputSplit>) factory
          .getDeserializer(orbConf.getClassByName(splitClass));
      deserializer.open(splitBuffer);
      split = deserializer.deserialize(null);
      JobConf job = new JobConf(orbConf);
      JobContext jobContext = new JobContext(job, new JobID(getOrbConf().getJobNumber(), 0));
      InputFormat<INPUT_KEY,INPUT_VALUE> inputFormat;
      inputFormat = (InputFormat<INPUT_KEY,INPUT_VALUE>) ReflectionUtils.newInstance(
        jobContext.getInputFormatClass(), orbConf);
      TaskAttemptContext tao = new TaskAttemptContext(job, new TaskAttemptID(new TaskID(
          jobContext.getJobID(), true, partitionID), 0));
      recordReader = inputFormat.createRecordReader(split, tao);
      recordReader.initialize(split, tao);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    
  }
  
  /**
   * Return the recordReader.
   */
  public RecordReader<INPUT_KEY,INPUT_VALUE> getRecordReader() {
    return recordReader;
  }
  
  /**
   * Set the orbConf.
   * 
   * @param orbConf
   */
  public void setOrbConf(OrbConfiguration orbConf) {
    this.orbConf = orbConf;
  }
  
  /**
   * Return the orbConf.
   */
  public OrbConfiguration getOrbConf() {
    return orbConf;
  }
  
  /**
   * Return the rawSplit.
   */
  public BytesWritable getRawSplit() {
    return rawSplit;
  }
  
  /**
   * Set the rawSplit.
   * 
   * @param rawSplit
   *          - BytesWritable
   */
  public void setRawSplit(BytesWritable rawSplit) {
    this.rawSplit = rawSplit;
  }
  
  /**
   * Return the splitClass
   */
  public String getSplitClass() {
    return splitClass;
  }
  
  /**
   * Set the splitClass
   * 
   * @param String
   *          splitClass
   */
  public void setSplitClass(String splitClass) {
    this.splitClass = splitClass;
  }
  
  /**
   * Return the partitionID
   */
  public int getPartitionID() {
    return partitionID;
  }
  
  /**
   * Set the partitionID
   * 
   * @param int partitionID
   */
  public void setPartitionID(int partitionID) {
    this.partitionID = partitionID;
  }
}
