package org.goldenorb.algorithms.maximumValue;

import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.goldenorb.Edge;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.io.input.VertexBuilder;

public class MaximumValueVertexReader extends VertexBuilder<MaximumValueVertex,LongWritable,Text>{


  @Override
  public MaximumValueVertex buildVertex(LongWritable key, Text value) {
    String[] values = value.toString().split("\t");
    ArrayList<Edge<IntWritable>> edgeCollection = new ArrayList<Edge<IntWritable>>();
    for(int i=2; i < values.length; i++){
      Edge<IntWritable> edge = new Edge<IntWritable>(Integer.toString(Integer.parseInt(values[i].trim())), new IntWritable(0));
      edgeCollection.add(edge);
    }
    
    MaximumValueVertex vertex = new MaximumValueVertex(Integer.toString(Integer.parseInt(values[0].trim())), new IntWritable(Integer.parseInt(values[1].trim())), edgeCollection);
//    System.out.println(vertex);
    return vertex;
  }

}
