package org.goldenorb.algorithms.pageRank;


import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.goldenorb.Edge;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.io.input.VertexBuilder;

public class PageRankVertexReader extends VertexBuilder<PageRankVertex,LongWritable,Text>{


  @Override
	public PageRankVertex buildVertex(LongWritable key, Text value) {
		String[] values = value.toString().split("\t");
		ArrayList<Edge<IntWritable>> edgeCollection = new ArrayList<Edge<IntWritable>>();
		for(int i=1; i < values.length; i++){
			Edge<IntWritable> edge = new Edge<IntWritable>(Integer.toString(Integer.parseInt(values[i].trim())), new IntWritable(0));
			edgeCollection.add(edge);
		}
		
		//String _vertexID, DoubleWritable _value, List<Edge<IntWritable>> _edges
		PageRankVertex vertex = new PageRankVertex(Integer.toString(Integer.parseInt(values[0].trim())), new DoubleWritable(), edgeCollection);
		
		return vertex;
	}

}
