package org.goldenorb.io.output;

import org.goldenorb.Vertex;

public abstract class VertexWriter<V extends Vertex, KEY, VALUE> {
	public abstract OrbContext<KEY, VALUE> vertexWrite(V vertex);
}
