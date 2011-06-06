package org.goldenorb.types.message;

import org.apache.hadoop.io.IntWritable;
import org.goldenorb.Message;

public class IntMessage extends Message<IntWritable>{
	public IntMessage(){
		super(IntWritable.class);
	}
	public IntMessage(String destinationVertex, IntWritable messageValue){
		super(IntWritable.class);
		this.setDestinationVertex(destinationVertex);
		this.setMessageValue(messageValue);
	}
	public IntMessage(String destinationVertex, int value){
		super(IntWritable.class);
		this.setDestinationVertex(destinationVertex);
		this.setMessageValue(new IntWritable(value));
	}
	public int get(){
		return ((IntWritable)this.getMessageValue()).get();
	}
	public void set(int value){
		((IntWritable)this.getMessageValue()).set(value);
	}
}
