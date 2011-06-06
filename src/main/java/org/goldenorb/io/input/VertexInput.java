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
public class VertexInput<INPUT_KEY, INPUT_VALUE> implements OrbConfigurable {
	
	private OrbConfiguration orbConf;
	private BytesWritable rawSplit;
	private String splitClass;
	private int partitionID;
	private RecordReader<INPUT_KEY, INPUT_VALUE> recordReader;
	
	public VertexInput(){}
	
	@SuppressWarnings("unchecked")
	public void initialize(){
		// rebuild the input split
	    org.apache.hadoop.mapreduce.InputSplit split = null;
	    DataInputBuffer splitBuffer = new DataInputBuffer();
	    splitBuffer.reset(rawSplit.getBytes(), 0, rawSplit.getLength());
	    SerializationFactory factory = new SerializationFactory(orbConf);
	    Deserializer<? extends org.apache.hadoop.mapreduce.InputSplit> deserializer;
		try {
			deserializer = (Deserializer<? extends org.apache.hadoop.mapreduce.InputSplit>) 
	        factory.getDeserializer(orbConf.getClassByName(splitClass));
			deserializer.open(splitBuffer);
		    split = deserializer.deserialize(null);
			JobConf job = new JobConf(orbConf);
			JobContext jobContext = new JobContext(job, new JobID(getOrbConf().getOrbJobName(),0));
			InputFormat<INPUT_KEY, INPUT_VALUE> inputFormat;
			inputFormat = (InputFormat<INPUT_KEY, INPUT_VALUE>) ReflectionUtils.newInstance(jobContext.getInputFormatClass(), orbConf);
			TaskAttemptContext tao = new TaskAttemptContext(job, new TaskAttemptID(new TaskID(jobContext.getJobID(), true, partitionID), 0));
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
	
	public RecordReader<INPUT_KEY, INPUT_VALUE> getRecordReader() {
		return recordReader;
	}
	
	public void setOrbConf(OrbConfiguration orbConf) {
		this.orbConf = orbConf;
	}

	public OrbConfiguration getOrbConf() {
		return orbConf;
	}
	public BytesWritable getRawSplit() {
		return rawSplit;
	}
	
	public void setRawSplit(BytesWritable rawSplit) {
		this.rawSplit = rawSplit;
	}
	
	public String getSplitClass() {
		return splitClass;
	}
	
	public void setSplitClass(String splitClass) {
		this.splitClass = splitClass;
	}
	
	public int getPartitionID() {
		return partitionID;
	}
	
	public void setPartitionID(int partitionID) {
		this.partitionID = partitionID;
	}
}
