package org.goldenorb.algorithms.semiclustering;


import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.goldenorb.Edge;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.io.input.VertexBuilder;

public class SemiClusteringVertexReader extends VertexBuilder<SemiClusteringVertex,LongWritable,Text>{


  @Override
	public SemiClusteringVertex buildVertex(LongWritable key, Text value) {
		String[] values = value.toString().split("\t");
		ArrayList<Edge<IntWritable>> edgeCollection = new ArrayList<Edge<IntWritable>>();
		for(int i=1; i < values.length; i++){
			Edge<IntWritable> edge = new Edge<IntWritable>(Integer.toString(Integer.parseInt(values[i].trim())), new IntWritable(0));
			edgeCollection.add(edge);
		}
		
		//String _vertexID, DoubleWritable _value, List<Edge<IntWritable>> _edges
		SemiClusteringVertex vertex = new SemiClusteringVertex(Integer.toString(Integer.parseInt(values[0].trim())), null, edgeCollection);
		
		return vertex;
	}

}
