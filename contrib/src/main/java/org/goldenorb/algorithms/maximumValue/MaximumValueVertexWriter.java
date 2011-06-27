package org.goldenorb.algorithms.maximumValue;

import org.apache.hadoop.io.Text;
import org.goldenorb.io.output.OrbContext;
import org.goldenorb.io.output.VertexWriter;

public class MaximumValueVertexWriter extends VertexWriter<MaximumValueVertex, Text, Text> {

	@Override
	public OrbContext<Text, Text> vertexWrite(MaximumValueVertex vertex) {
		// TODO Auto-generated method stub
		OrbContext<Text, Text> orbContext = new OrbContext<Text, Text>();
		orbContext.write(new Text(vertex.getVertexID()), new Text(Integer.toString(vertex.getMaxValue())));
		
		return orbContext;
	}

}
