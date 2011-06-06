package org.goldenorb.types.message;

import org.apache.hadoop.io.LongWritable;
import org.goldenorb.Message;

public class LongMessage extends Message<LongWritable> {
	public LongMessage(){
		super(LongWritable.class);
	}
	public LongMessage(String destinationVertex, LongWritable messageValue){
		super(LongWritable.class);
		this.setDestinationVertex(destinationVertex);
		this.setMessageValue(messageValue);
	}
	public LongMessage(String destinationVertex, long value){
		super(LongWritable.class);
		this.setDestinationVertex(destinationVertex);
		this.setMessageValue(new LongWritable(value));
	}
	public long get(){
		return ((LongWritable)this.getMessageValue()).get();
	}
	public void set(long value){
		((LongWritable)this.getMessageValue()).set(value);
	}
}
