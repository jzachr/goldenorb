package org.goldenorb.algorithms.singleSourceShortestPath;

import org.apache.hadoop.io.Text;
import org.goldenorb.io.output.OrbContext;
import org.goldenorb.io.output.VertexWriter;

public class SingleSourceShortestPathWriter extends VertexWriter<SingleSourceShortestPathVertex, Text, Text> {

	@Override
	public OrbContext<Text, Text> vertexWrite(SingleSourceShortestPathVertex vertex) {
		// TODO Auto-generated method stub
		OrbContext<Text, Text> orbContext = new OrbContext<Text, Text>();
		orbContext.write(new Text(vertex.getVertexID()), new Text(vertex.toString()));
		
		return orbContext;
	}

}
