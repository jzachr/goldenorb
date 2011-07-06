package org.goldenorb.algorithms.semiclustering;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.goldenorb.OrbRunner;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.types.message.IntMessage;

public class OrbSemiClusteringJob extends OrbRunner{
	
	public static String MAXCLUSTERS = "semiclustering.maxclusters";
	public static String MAXVERTICES = "semiclustering.maxvertices";
	public static String MAXITERATIONS = "semiclustering.maxiterations";
	public static String BOUNDARYFACTOR = "semiclustering.boundaryfactor";
		
	private OrbConfiguration orbConf;
	
	public static void main(String[] args){
		
		String inputpath = args[0];
		String outputpath = args[1];
		String maxclusters = args[2];
		String maxvertices = args[3];
		String maxiterations = args[4];
		String boundaryfactor = args[5];
		
		OrbSemiClusteringJob oscj = new OrbSemiClusteringJob();
		oscj.startJob(inputpath, outputpath, maxclusters, maxvertices, maxiterations, boundaryfactor);
	}
	
	public void startJob(String inputPath, String outputPath, String maxclusters, String maxvertices, String maxiterations, String boundaryfactor){
		
		orbConf = new OrbConfiguration(true);
		
		orbConf.setFileInputFormatClass(TextInputFormat.class);
		orbConf.setFileOutputFormatClass(TextOutputFormat.class);
		orbConf.setVertexClass(SemiClusteringVertex.class);
		orbConf.setMessageClass(IntMessage.class);
		orbConf.setVertexInputFormatClass(SemiClusteringVertexReader.class);
		orbConf.setVertexOutputFormatClass(SemiClusteringVertexWriter.class);
		orbConf.setNumberOfMessageHandlers(10);
		orbConf.setNumberOfVertexThreads(10);
		orbConf.setNumberOfPartitionsPerMachine(4);
		
		orbConf.set(MAXCLUSTERS, maxclusters);
		orbConf.set(MAXVERTICES, maxvertices);
		orbConf.set(BOUNDARYFACTOR, boundaryfactor);
		orbConf.set(MAXITERATIONS, maxiterations);
		runJob(orbConf);
		
	}
}
