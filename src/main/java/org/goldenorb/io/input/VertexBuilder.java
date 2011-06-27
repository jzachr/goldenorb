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
import org.apache.hadoop.io.Writable;
import org.goldenorb.Vertex;
import org.goldenorb.conf.OrbConfigurable;
import org.goldenorb.conf.OrbConfiguration;


/**
 * This class provides an abstract framework to implement a VertexBuilder.
 *
 * @param <VERTEX>
 * @param <INPUT_KEY>
 * @param <INPUT_VALUE>
 */
public abstract class VertexBuilder<VERTEX extends Vertex<?,?,?>, INPUT_KEY extends Writable, INPUT_VALUE extends Writable> implements OrbConfigurable{
	
	private OrbConfiguration orbConf;
	private VertexInput<INPUT_KEY, INPUT_VALUE> vertexInput;
	
/**
 * 
 * @param key - INPUT_KEY
 * @param value - INPUT_VALUE
 * @returns VERTEX
 */
	protected abstract VERTEX buildVertex(INPUT_KEY key, INPUT_VALUE value);
	
/**
 * Constructor
 *
 */
	public VertexBuilder(){
		vertexInput = new VertexInput<INPUT_KEY, INPUT_VALUE>();
	}
	
/**
 * 
 */
	public void initialize(){
		vertexInput.setOrbConf(orbConf);
		vertexInput.initialize();
	}
	
/**
 * Return the orbConf.
 */
	public OrbConfiguration getOrbConf(){
		return orbConf;
	}
	
/**
 * Set the orbConf.
 * @param orbConf
 */
	public void setOrbConf(OrbConfiguration orbConf){
		this.orbConf = orbConf;
	}
	
/**
 * 
 * @returns boolean
 */
	public boolean nextVertex() throws IOException, InterruptedException{
		return vertexInput.getRecordReader().nextKeyValue();
	}
	
/**
 * Return the currentVertex
 */
	public VERTEX getCurrentVertex() throws IOException, InterruptedException{
		return buildVertex(vertexInput.getRecordReader().getCurrentKey(), vertexInput.getRecordReader().getCurrentValue());
	}
	
/**
 * Set the rawSplit.
 * @param rawSplit - BytesWritable
 */
	public void setRawSplit(BytesWritable rawSplit) {
		vertexInput.setRawSplit(rawSplit);
	}
	
/**
 * Set the splitClass
 * @param splitClass - String
 */
	public void setSplitClass(String splitClass) {
		vertexInput.setSplitClass(splitClass);
	}
	
/**
 * Set the partitionID
 * @param partitionID - int
 */
	public void setPartitionID(int partitionID) {
		vertexInput.setPartitionID(partitionID);
	}
}
