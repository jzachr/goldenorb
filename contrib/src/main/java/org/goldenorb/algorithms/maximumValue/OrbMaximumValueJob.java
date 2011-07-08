package org.goldenorb.algorithms.maximumValue;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.goldenorb.OrbRunner;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.types.message.IntMessage;

public class OrbMaximumValueJob extends OrbRunner{
	public static void main(String[] args){
	  if (args.length != 5) {
	    System.out.println("Missing arguments");
	    System.out.println("usage: OrbMaximumValueJob input-dir output-dir requested-partitions reserved-partitions classpath");
	    System.exit(-1);
	  }
	  String inDir = args[0];
	  String outDir = args[1];
	  int reqP = Integer.parseInt(args[2]);
	  int resP = Integer.parseInt(args[3]);
	  String cp = args[4];
	  
		OrbMaximumValueJob omvj = new OrbMaximumValueJob();
		omvj.startJob(inDir, outDir, reqP, resP, cp);
	}
	
	private void startJob(String inputDir, String outputDir, int requested, int reserved, String classPath){
		OrbConfiguration orbConf = new OrbConfiguration(true);
		
		orbConf.setFileInputFormatClass(TextInputFormat.class);
		orbConf.setFileOutputFormatClass(TextOutputFormat.class);
		orbConf.setVertexClass(MaximumValueVertex.class);
		orbConf.setMessageClass(IntMessage.class);
		orbConf.setVertexInputFormatClass(MaximumValueVertexReader.class);
		orbConf.setVertexOutputFormatClass(MaximumValueVertexWriter.class);
		orbConf.setNumberOfMessageHandlers(10);
		orbConf.setNumberOfVertexThreads(10);
		
		orbConf.setFileInputPath(inputDir);
		orbConf.setFileOutputPath(outputDir);
		
		orbConf.setOrbRequestedPartitions(requested);
		orbConf.setOrbReservedPartitions(reserved);
		
		orbConf.setOrbClassPaths(classPath);
		
		runJob(orbConf);
	}
}
