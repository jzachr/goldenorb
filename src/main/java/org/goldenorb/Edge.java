package org.goldenorb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Edge<EV extends Writable> implements Writable{
	
	private String destinationVertex;
	private EV edgeValue;
	
	private Class<EV> edgeType;
	
	public Edge(Class<EV> _edgeType)
	{
		edgeType = _edgeType;
	}
	
	public Edge(String _destinationVertex, EV _edgeValue)
	{
		edgeValue = _edgeValue;
		destinationVertex = _destinationVertex;
	}
	
	public String getDestinationVertex() {
		return destinationVertex;
	}
	public void setDestinationVertex(String destinationVertex) {
		this.destinationVertex = destinationVertex;
	}
	public EV getEdgeValue() {
		return edgeValue;
	}
	public void setEdgeValue(EV edgeValue) {
		this.edgeValue = edgeValue;
	}

	public void readFields(DataInput in) throws IOException {
		destinationVertex = in.readUTF();
		try {
			edgeValue = (EV) edgeType.newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		edgeValue.readFields(in);
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(destinationVertex);
		edgeValue.write(out);
	}
}
