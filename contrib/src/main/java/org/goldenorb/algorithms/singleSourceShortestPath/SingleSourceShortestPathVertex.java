package org.goldenorb.algorithms.singleSourceShortestPath;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableUtils;
import org.goldenorb.Edge;
import org.goldenorb.Vertex;

public class SingleSourceShortestPathVertex extends Vertex<PathWritable,IntWritable,PathMessage> {
  
  PathWritable shortestPath;
  
  public SingleSourceShortestPathVertex() {
    super(PathWritable.class, IntWritable.class, PathMessage.class);
    shortestPath = new PathWritable();
  }
  
  public SingleSourceShortestPathVertex(String _vertexID, PathWritable _value, List<Edge<IntWritable>> _edges) {
    super(_vertexID, _value, _edges);
    shortestPath = _value;
  }
  
  @Override
  public void compute(Collection<PathMessage> messages) {
    int _minWeight = shortestPath.getWeight().get();
    PathWritable _path = null;
    String sourceVertex = this.getOci().getOrbProperty(
      OrbSingleSourceShortestPathJob.ALGORITHM_NAME + "." + OrbSingleSourceShortestPathJob.SOURCE_VERTEX);
    
    if (messages.size() == 0 && this.getVertexID().equalsIgnoreCase(sourceVertex)
        && this.getOci().superStep() == 1) {
      _minWeight = 0;
    }
    
    for (PathMessage m : messages) {
      int msgValue = ((PathWritable) m.getMessageValue()).getWeight().get();
      if (msgValue < _minWeight) {
        _path = WritableUtils.clone((PathWritable) m.getMessageValue(), new Configuration(true));
        _minWeight = _path.getWeight().get();
      }
    }
    
    if (_minWeight < shortestPath.getWeight().get()) {
      if (_path != null) {
        shortestPath = _path;
      } else {
        shortestPath.setWeight(new IntWritable(_minWeight));
      }
      for (Edge<IntWritable> e : getEdges()) {
        PathWritable _outpath = WritableUtils.clone(shortestPath, new Configuration(true));
        _outpath.addVertex(this);
        _outpath.setWeight(new IntWritable(shortestPath.getWeight().get() + e.getEdgeValue().get()));
        PathMessage message = new PathMessage(e.getDestinationVertex(), _outpath);
        sendMessage(message);
      }
    }
    
    this.voteToHalt();
  }
  
  public int getShortestPath() {
    return shortestPath.getWeight().get();
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Distance: " + shortestPath.getWeight().get());
    sb.append(" Path: " + shortestPath.getVertices().getArrayList().toString());
    return sb.toString();
  }
}
