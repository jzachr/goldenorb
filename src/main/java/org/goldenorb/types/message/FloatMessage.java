package org.goldenorb.types.message;

import org.apache.hadoop.io.FloatWritable;
import org.goldenorb.Message;

public class FloatMessage extends Message<FloatWritable> {
	public FloatMessage(){
		super(FloatWritable.class);
	}
	public FloatMessage(String destinationVertex, FloatWritable messageValue){
		super(FloatWritable.class);
		this.setDestinationVertex(destinationVertex);
		this.setMessageValue(messageValue);
	}
	public FloatMessage(String destinationVertex, float value){
		super(FloatWritable.class);
		this.setDestinationVertex(destinationVertex);
		this.setMessageValue(new FloatWritable(value));
	}
	public float get(){
		return ((FloatWritable)this.getMessageValue()).get();
	}
	public void set(float value){
		((FloatWritable)this.getMessageValue()).set(value);
	}
}
