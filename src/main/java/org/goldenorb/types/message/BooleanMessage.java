package org.goldenorb.types.message;

import org.apache.hadoop.io.BooleanWritable;
import org.goldenorb.Message;


public class BooleanMessage extends Message<BooleanWritable> {
	public BooleanMessage(){
		super(BooleanWritable.class);
	}
	public BooleanMessage(String destinationVertex, BooleanWritable messageValue){
		super(BooleanWritable.class);
		this.setDestinationVertex(destinationVertex);
		this.setMessageValue(messageValue);
	}
	public BooleanMessage(String destinationVertex, boolean value){
		super(BooleanWritable.class);
		this.setDestinationVertex(destinationVertex);
		this.setMessageValue(new BooleanWritable(value));
	}
	public boolean get(){
		return ((BooleanWritable)this.getMessageValue()).get();
	}
	public void set(boolean value){
		((BooleanWritable)this.getMessageValue()).set(value);
	}
}
