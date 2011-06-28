package org.goldenorb.algorithms.singleSourceShortestPath;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.goldenorb.OrbRunner;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.types.message.IntMessage;

public class OrbSingleSourceShortestPathJob extends OrbRunner{
	
	private OrbConfiguration orbConf;
	
	public static void main(String[] args){
		
		String inputpath = args[0];
		String outputpath = args[1];
		String classpath = args[2];
		OrbSingleSourceShortestPathJob omvj = new OrbSingleSourceShortestPathJob();
		omvj.startJob(inputpath, outputpath, classpath);
	}
	
	public void startJob(String inputPath, String outputPath, String classPath){
		
		orbConf = new OrbConfiguration(true);
		
		orbConf.setFileInputFormatClass(TextInputFormat.class);
		orbConf.setFileOutputFormatClass(TextOutputFormat.class);
		orbConf.setVertexClass(SingleSourceShortestPathVertex.class);
		orbConf.setMessageClass(IntMessage.class);
		orbConf.setVertexInputFormatClass(SingleSourceShortestPathReader.class);
		orbConf.setVertexOutputFormatClass(SingleSourceShortestPathWriter.class);
		orbConf.setNumberOfMessageHandlers(10);
		orbConf.setNumberOfVertexThreads(10);
		orbConf.setNumberOfPartitionsPerMachine(4);
		runJob(orbConf);
		
	}
}
