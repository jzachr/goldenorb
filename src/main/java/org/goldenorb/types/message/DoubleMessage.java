package org.goldenorb.types.message;

import org.apache.hadoop.io.DoubleWritable;
import org.goldenorb.Message;

public class DoubleMessage extends Message<DoubleWritable> {
	public DoubleMessage(){
		super(DoubleWritable.class);
	}
	public DoubleMessage(String destinationVertex, DoubleWritable messageValue){
		super(DoubleWritable.class);
		this.setDestinationVertex(destinationVertex);
		this.setMessageValue(messageValue);
	}
	public DoubleMessage(String destinationVertex, double value){
		super(DoubleWritable.class);
		this.setDestinationVertex(destinationVertex);
		this.setMessageValue(new DoubleWritable(value));
	}
	public double get(){
		return ((DoubleWritable)this.getMessageValue()).get();
	}
	public void set(double value){
		((DoubleWritable)this.getMessageValue()).set(value);
	}
}
