package org.goldenorb.util.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.goldenorb.Message;
import org.goldenorb.Vertex;

@SuppressWarnings("rawtypes")
public class SourceMessage<MV extends Writable> extends Message {

	private String sourceVertex;

	public SourceMessage() {}
	  
	/**
	 * Constructor
	 *
	 * @param  Class<MV> messageValueClass
	 */
	  public SourceMessage(Vertex v, Class<MV> messageValueClass) {
	    super(messageValueClass);
	    sourceVertex = v.vertexID();
	  }
	  
	public String getSourceVertex() {
		return sourceVertex;
	}

	public void setSourceVertex(String sourceVertex) {
		this.sourceVertex = sourceVertex;
	}
	
	/**
	 * 
	 * @returns String
	 */
	  @Override
	  public String toString() {
	    return "source: \"" + sourceVertex + "\", " + super.toString();
	  }
	  
	/**
	 * Deserialize the fields of this object from in.
	 * @param  DataInput in
	 */
	  public void readFields(DataInput in) throws IOException {
	    sourceVertex = in.readUTF();
	    super.readFields(in);
	  }
	  
	/**
	 * 
	 * @param  DataOutput out
	 */
	  public void write(DataOutput out) throws IOException {
	    out.writeUTF(sourceVertex);
	    super.write(out);
	  }
	
}
