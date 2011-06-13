package org.goldenorb;

@SuppressWarnings("rawtypes")
public class Vertices extends ArrayListWritable<Vertex>{
	public void setVertexType(Class<? extends Vertex> vertexType){
		this.setWritableType(vertexType);
	}
}
