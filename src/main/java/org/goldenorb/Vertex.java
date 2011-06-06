package org.goldenorb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.io.Writable;


public abstract class Vertex<VV extends Writable, EV extends Writable, MV extends Message<? extends Writable>> implements Writable{

	private OrbPartition.OrbCommunicationInterface oci;	

	private String vertexID;
	private Collection<Edge<EV>> edges;
	private VV value;
	
	private Class<VV> vertexValue;
	private Class<EV> edgeValue;
	
	public Vertex()
	{
		
	}
	
	public Vertex(OrbPartition.OrbCommunicationInterface _oci, String _vertexID, VV _value, Collection<Edge<EV>> _edges)
	{
		oci = _oci;
		vertexID = _vertexID;
		value = _value;
		edges = _edges;
	}
	
	public Vertex(String _vertexID, VV _value, Collection<Edge<EV>> _edges){
		vertexID = _vertexID;
		value = _value;
		edges = _edges;
	}
	
	public Vertex(OrbPartition.OrbCommunicationInterface _oci, Class<VV> _vertexValue, Class<EV> _edgeValue, Class<MV> _messageValue)
	{
		oci = _oci;
		vertexValue = _vertexValue;
		edgeValue = _edgeValue;
	}
	
	public Vertex(Class<VV> _vertexValue, Class<EV> _edgeValue, Class<MV> _messageValue)
	{
		vertexValue = _vertexValue;
		edgeValue = _edgeValue;
	}
	
	public String vertexID() {
		return vertexID;
	}

	public abstract void compute(Collection<MV> messages);

	protected long superStep() {
		return oci.superStep();
	}

	protected void setValue(VV value){
		this.value = value;
	}
	
	public VV getValue() {
		return value;
	}

	protected Collection<Edge<EV>> getEdges() {
		return edges;
	}

	public void sendMessage(MV message) {
		oci.sendMessage(message);
	}

	public void voteToHalt() {
		oci.voteToHalt(vertexID);
	}
	
	public OrbPartition.OrbCommunicationInterface getOci() {
		return oci;
	}

	public void setOci(OrbPartition.OrbCommunicationInterface oci) {
		this.oci = oci;
	}
	
	public void readFields(DataInput in) throws IOException {
		vertexID = in.readUTF();
		
		value = (VV) newVertexValue();
		value.readFields(in);
		
		int numberOfEdges = in.readInt();
		ArrayList<Edge<EV>> _edges = new ArrayList<Edge<EV>>(numberOfEdges);
		for(int i = 0; i < numberOfEdges; i++)
		{
			Edge<EV> edge = new Edge<EV>(edgeValue);
			edge.readFields(in);
			_edges.add(edge);
		}
		
		edges = _edges;		
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(vertexID);
		value.write(out);
		out.writeInt(edges.size());
		for(Edge<EV> edge : edges)
		{
			edge.write(out);
		}
	}
	
	protected VV newVertexValue()
	{
		VV vv = null;
		try {
			vv = vertexValue.newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		
		return vv;
	}
	
	@Override
	public String toString()
	{
		return "Vertex: " + vertexID + "  Contents: { " + value.toString() + " }";
	}

	public String getVertexID() {
		return vertexID;
	}
}
