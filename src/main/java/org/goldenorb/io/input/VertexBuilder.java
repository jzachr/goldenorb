package org.goldenorb.io.input;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.goldenorb.Vertex;
import org.goldenorb.conf.OrbConfigurable;
import org.goldenorb.conf.OrbConfiguration;

public abstract class VertexBuilder<VERTEX extends Vertex<?,?,?>, INPUT_KEY extends Writable, INPUT_VALUE extends Writable> implements OrbConfigurable{
	
	private OrbConfiguration orbConf;
	private VertexInput<INPUT_KEY, INPUT_VALUE> vertexInput;
	
	protected abstract VERTEX buildVertex(INPUT_KEY key, INPUT_VALUE value);
	
	public VertexBuilder(){
		vertexInput = new VertexInput<INPUT_KEY, INPUT_VALUE>();
	}
	
	public void initialize(){
		vertexInput.setOrbConf(orbConf);
		vertexInput.initialize();
	}
	
	public OrbConfiguration getOrbConf(){
		return orbConf;
	}
	
	public void setOrbConf(OrbConfiguration orbConf){
		this.orbConf = orbConf;
	}
	
	public boolean nextVertex() throws IOException, InterruptedException{
		return vertexInput.getRecordReader().nextKeyValue();
	}
	
	public VERTEX getCurrentVertex() throws IOException, InterruptedException{
		return buildVertex(vertexInput.getRecordReader().getCurrentKey(), vertexInput.getRecordReader().getCurrentValue());
	}
	
	public void setRawSplit(BytesWritable rawSplit) {
		vertexInput.setRawSplit(rawSplit);
	}
	
	public void setSplitClass(String splitClass) {
		vertexInput.setSplitClass(splitClass);
	}
	
	public void setPartitionID(int partitionID) {
		vertexInput.setPartitionID(partitionID);
	}
}
