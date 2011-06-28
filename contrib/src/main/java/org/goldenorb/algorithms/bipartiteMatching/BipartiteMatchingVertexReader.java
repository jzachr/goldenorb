package org.goldenorb.algorithms.bipartiteMatching;


import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.goldenorb.Edge;
import org.goldenorb.io.input.VertexBuilder;

public class BipartiteMatchingVertexReader extends VertexBuilder<BipartiteMatchingVertex,LongWritable,Text>{


  @Override
	public BipartiteMatchingVertex buildVertex(LongWritable key, Text value) {
		String[] values = value.toString().split("\t");
		ArrayList<Edge<IntWritable>> edgeCollection = new ArrayList<Edge<IntWritable>>();
		for(int i=2; i < values.length; i++){
			Edge<IntWritable> edge = new Edge<IntWritable>(Integer.toString(Integer.parseInt(values[i].trim())), new IntWritable(0));
			edgeCollection.add(edge);
		}
		
		//String _vertexID, DoubleWritable _value, List<Edge<IntWritable>> _edges
		BipartiteMatchingVertex vertex = new BipartiteMatchingVertex(new String(values[0]+":"+values[1]), null, edgeCollection);
		
		return vertex;
	}

}
