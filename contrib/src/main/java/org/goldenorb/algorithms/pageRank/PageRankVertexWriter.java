package org.goldenorb.algorithms.pageRank;

import org.apache.hadoop.io.Text;
import org.goldenorb.io.output.OrbContext;
import org.goldenorb.io.output.VertexWriter;

public class PageRankVertexWriter extends VertexWriter<PageRankVertex, Text, Text> {

	@Override
	public OrbContext<Text, Text> vertexWrite(PageRankVertex vertex) {
		// TODO Auto-generated method stub
		OrbContext<Text, Text> orbContext = new OrbContext<Text, Text>();
		orbContext.write(new Text(vertex.getVertexID()), new Text(Double.toString(vertex.getPageRank())));
		
		return orbContext;
	}

}
