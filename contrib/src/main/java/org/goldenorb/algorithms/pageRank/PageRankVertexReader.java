package org.goldenorb.algorithms.pageRank;


import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.goldenorb.Edge;
import org.goldenorb.io.input.VertexBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageRankVertexReader extends VertexBuilder<PageRankVertex,LongWritable,Text>{

	private final static Logger logger = LoggerFactory.getLogger(PageRankVertexReader.class);
	
  @Override
	public PageRankVertex buildVertex(LongWritable key, Text value) {
		
	  logger.debug(key.toString());
	  logger.debug(value.toString());
		
	  	String[] values = value.toString().split("\t");
		ArrayList<Edge<IntWritable>> edgeCollection = new ArrayList<Edge<IntWritable>>();
		for(int i=1; i < values.length; i++){
			Edge<IntWritable> edge = new Edge<IntWritable>(Integer.toString(Integer.parseInt(values[i].trim())), new IntWritable(1));
			edgeCollection.add(edge);
		}
		
		logger.debug(values[0]);
		logger.debug(edgeCollection.toString());
		
		//String _vertexID, DoubleWritable _value, List<Edge<IntWritable>> _edges
		PageRankVertex vertex = new PageRankVertex(Integer.toString(Integer.parseInt(values[0].trim())), new DoubleWritable(0.0), edgeCollection);
		
		logger.debug(vertex.toString());
		
		return vertex;
	}

}
