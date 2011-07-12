package org.goldenorb.algorithms.singleSourceShortestPath;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.goldenorb.OrbRunner;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.types.message.IntMessage;

public class OrbSingleSourceShortestPathJob extends OrbRunner{
	
  public static final String SOURCE_VERTEX = "singlesourceshortestpath.source";
  
  public static void main(String[] args){
    if (args.length != 5) {
      System.out.println("Missing arguments");
      System.out.println("usage: OrbSingleSourceShortestPathJob input-dir output-dir requested-partitions reserved-partitions source-vertex");
      System.exit(-1);
    }
    String inDir = args[0];
    String outDir = args[1];
    int reqP = Integer.parseInt(args[2]);
    int resP = Integer.parseInt(args[3]);
    String src = args[4];
    
    OrbSingleSourceShortestPathJob job = new OrbSingleSourceShortestPathJob();
    job.startJob(inDir, outDir, reqP, resP, src);
  }

	
	public void startJob(String inputPath, String outputPath, int requested, int reserved, String sourceVertex) {
		OrbConfiguration orbConf = new OrbConfiguration(true);
		
		orbConf.setFileInputFormatClass(TextInputFormat.class);
		orbConf.setFileOutputFormatClass(TextOutputFormat.class);
		orbConf.setVertexClass(SingleSourceShortestPathVertex.class);
		orbConf.setMessageClass(PathMessage.class);
		orbConf.setVertexInputFormatClass(SingleSourceShortestPathReader.class);
		orbConf.setVertexOutputFormatClass(SingleSourceShortestPathWriter.class);
		orbConf.setNumberOfMessageHandlers(10);
		orbConf.setNumberOfVertexThreads(10);

		orbConf.setFileInputPath(inputPath);
		orbConf.setFileOutputPath(outputPath);
		orbConf.setOrbRequestedPartitions(requested);
		orbConf.setOrbReservedPartitions(reserved);
		
		orbConf.set(SOURCE_VERTEX, sourceVertex);
		
		runJob(orbConf);
	}
}
