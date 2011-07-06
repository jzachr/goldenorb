package org.goldenorb.algorithms.semiclustering;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.goldenorb.Edge;
import org.goldenorb.Vertex;
import org.goldenorb.types.ArrayListWritable;
import org.goldenorb.util.message.IntSourceMessage;

/*
 * Start of non-generated import declaration code -- any code written outside of this block will be
 * removed in subsequent code generations.
 */

/* End of non-generated import declaraction code */

/**
 * This class represents a path to the source
 */
@SuppressWarnings("rawtypes")
public class SemiClusterWritable implements
		org.apache.hadoop.io.WritableComparable {

	/**
	 * aggregate metrics of this cluster
	 */
	private DoubleWritable score = new DoubleWritable(Double.MIN_VALUE);
	private IntWritable vertexCount = new IntWritable(1);
	
	/**
	 * the vertices in this cluster
	 */
	private ArrayListWritable vertices = new ArrayListWritable();

	/*
	 * Start of non-generated variable declaration code -- any code written
	 * outside of this block will be removed in subsequent code generations.
	 */

	/* End of non-generated variable declaraction code */

	/**
   * 
   */
	public SemiClusterWritable() {

	}

	/*
	 * Start of non-generated method code -- any code written outside of this
	 * block will be removed in subsequent code generations.
	 */

	/* End of non-generated method code */

	public DoubleWritable getScore() {
		return score;
	}

	public void setScore(DoubleWritable score) {
		this.score = score;
	}

	public int getVertexCount() {
		return getVertices().size();
	}

	public int getInternalWeight() {
		int internalWeight = 0;
		for (SemiClusteringVertex v : getVertices()) {
			for (Edge<IntWritable> e : v.getEdges()) {
				if( getVertexIds().contains(e.getDestinationVertex()))
					internalWeight += e.getEdgeValue().get();
			}
		}
		return internalWeight;
	}

	public int getBoundaryWeight() {
		
		int boundaryWeight = 0;
		for (SemiClusteringVertex v : getVertices()) {
			for (Edge<IntWritable> e : v.getEdges()) {
				if( !getVertexIds().contains(e.getDestinationVertex()))
					boundaryWeight += e.getEdgeValue().get();
			}
		}
		return boundaryWeight;
	}

	/**
	 * gets the vertices in this cluster
	 * 
	 * @return
	 */
	public Collection<SemiClusteringVertex> getVertices() {
		return (Collection) vertices;
	}

	/**
	 * gets the vertices in this cluster
	 * 
	 * @return
	 */
	public Collection<String> getVertexIds() {
		ArrayList<String> ids = new ArrayList<String>();
		for (SemiClusteringVertex v : getVertices()) {
			ids.add(v.getVertexID());
		}
		return (Collection) ids;
	}
	/**
	 * adds a vertex to this path
	 * 
	 * @param vertices
	 */
	public void addVertex(Vertex vertex) {
		this.vertices.add(vertex);
	}

	// /////////////////////////////////////
	// Writable
	// /////////////////////////////////////
	public void readFields(DataInput in) throws IOException {
		score.readFields(in);
		vertices.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		score.write(out);
		vertices.write(out);
	}

	// /////////////////////////////////////
	// Comparable
	// /////////////////////////////////////
	public int compareTo(Object o) {
		return score.compareTo(((SemiClusterWritable) o).score);
	}

	public boolean equals(Object o) {
		return score.equals(((SemiClusterWritable) o).score)
				&& vertices.equals(((SemiClusterWritable) o).vertices);
	}
}
