package org.goldenorb.algorithms.singleSourceShortestPath;

import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.goldenorb.Edge;
import org.goldenorb.io.input.VertexBuilder;

public class SingleSourceShortestPathReader extends
    VertexBuilder<SingleSourceShortestPathVertex,LongWritable,Text> {
  
  @Override
  public SingleSourceShortestPathVertex buildVertex(LongWritable key, Text value) {
    String[] values = value.toString().split("\t");
    ArrayList<Edge<IntWritable>> edgeCollection = new ArrayList<Edge<IntWritable>>();
    for (int i = 1; i < values.length; i++) {
      String[] split = values[i].split(":");
      Edge<IntWritable> edge = new Edge<IntWritable>(Integer.toString(Integer.parseInt(split[0].trim())),
          new IntWritable(Integer.parseInt(split[1])));
      edgeCollection.add(edge);
    }
    
    PathWritable emptyPathWritable = new PathWritable();
    
    SingleSourceShortestPathVertex vertex = new SingleSourceShortestPathVertex(Integer.toString(Integer
        .parseInt(values[0].trim())), emptyPathWritable, edgeCollection);
    
    return vertex;
  }
  
}
