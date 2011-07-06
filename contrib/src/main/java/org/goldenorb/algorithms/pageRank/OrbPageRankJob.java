package org.goldenorb.algorithms.pageRank;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.goldenorb.OrbRunner;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.types.message.IntMessage;

public class OrbPageRankJob extends OrbRunner{
	
	public static String TOTALPAGES = "pagerank.totalpages";
	public static String MAXITERATIONS = "pagerank.maxiterations";
	public static String DAMPINGFACTOR = "semiclustering.boundaryfactor";
	
	private OrbConfiguration orbConf;
	
	public static void main(String[] args){
		
		String inputpath = args[0];
		String outputpath = args[1];
		String totalpages = args[2];
		String maxiterations = args[3];
		String dampingfactor = args[4];
		OrbPageRankJob omvj = new OrbPageRankJob();
		omvj.startJob(inputpath, outputpath, totalpages, maxiterations, dampingfactor);
	}
	
	public void startJob(String inputPath, String outputPath, String totalPages, String maxiterations, String dampingfactor){
		
		orbConf = new OrbConfiguration(true);
		
		orbConf.setFileInputFormatClass(TextInputFormat.class);
		orbConf.setFileOutputFormatClass(TextOutputFormat.class);
		orbConf.setVertexClass(PageRankVertex.class);
		orbConf.setMessageClass(IntMessage.class);
		orbConf.setVertexInputFormatClass(PageRankVertexReader.class);
		orbConf.setVertexOutputFormatClass(PageRankVertexWriter.class);
		orbConf.setNumberOfMessageHandlers(10);
		orbConf.setNumberOfVertexThreads(10);
		orbConf.setNumberOfPartitionsPerMachine(4);
		
		orbConf.set(TOTALPAGES, totalPages);
		orbConf.set(MAXITERATIONS, maxiterations);
		orbConf.set(DAMPINGFACTOR, dampingfactor);
		
		runJob(orbConf);
		
	}
}
