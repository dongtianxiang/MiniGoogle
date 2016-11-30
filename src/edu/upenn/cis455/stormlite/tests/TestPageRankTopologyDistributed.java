package edu.upenn.cis455.stormlite.tests;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Arrays;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.bolts.PRMapBolt;
import edu.upenn.cis.stormlite.bolts.PRReduceBolt;
import edu.upenn.cis.stormlite.bolts.PRResultBolt;
import edu.upenn.cis.stormlite.infrastructure.Configuration;
import edu.upenn.cis.stormlite.infrastructure.Topology;
import edu.upenn.cis.stormlite.infrastructure.TopologyBuilder;
import edu.upenn.cis.stormlite.infrastructure.WorkerJob;
import edu.upenn.cis.stormlite.spouts.RankFileSpout;
import edu.upenn.cis.stormlite.spouts.RankSpout;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis455.mapreduce.servlets.MasterServlet;

public class TestPageRankTopologyDistributed {
	
	private static final String SPOUT  = "LINK_SPOUT";
	private static final String MAP_BOLT = "MAP_BOLT";
	private static final String REDUCE_BOLT = "REDUCE_BOLT";
	private static final String RESULT_BOLT  = "RES_BOLT";

	public static void main(String[] args) throws IOException {
		
		int numMappers  = 1;
		int numReducers = 1;
		
		String jobClass  = "edu.upenn.cis455.mapreduce.jobs.PageRankJob" ;
		String inputDir  = "data1" ; 
		String outputDir = "result1";
		String jobName   = "JOB1"; 
				
		RankFileSpout spout = new RankSpout();		
	    PRMapBolt mapBolt = new PRMapBolt();
	    PRReduceBolt reduceBolt = new PRReduceBolt();
	    PRResultBolt printer = new PRResultBolt();
	    
	    // build topology
		TopologyBuilder builder = new TopologyBuilder();			    			    
        builder.setSpout(SPOUT, spout, 1);		        
        builder.setBolt(MAP_BOLT, mapBolt, numMappers).fieldsGrouping(SPOUT, new Fields("value"));		        
        builder.setBolt(REDUCE_BOLT, reduceBolt, numReducers).fieldsGrouping(MAP_BOLT, new Fields("key"));
        builder.setBolt(RESULT_BOLT, printer, 1).shuffleGrouping(REDUCE_BOLT);		        
        Topology topo = builder.createTopology();
        
        // create configuration object
        Configuration config = new Configuration();	        		        
        
        config.put("mapClass", jobClass);
        config.put("reduceClass", jobClass);
        config.put("spoutExecutors", "1");
        config.put("mapExecutors",    (new Integer(numMappers)).toString());
        config.put("reduceExecutors", (new Integer(numReducers)).toString());
        config.put("inputDir", inputDir);
        config.put("outputDir", outputDir);
        config.put("job", jobName);
        
        config.put("workers", "2");       
        config.put("decayFactor", "0.85");
        
        config.put("graphDataDir", "graphStore");
        config.put("databaseDir" , "storage");
        
        WorkerJob job = new WorkerJob(topo, config);
        ObjectMapper mapper = new ObjectMapper();	        
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        
        String[] workersList = new String[]{"127.0.0.1:8000", "127.0.0.1:8001"};
        
        config.put("workerList", Arrays.toString(workersList));		        
        
		try {
			int j = 0;
			for (String dest: workersList) {
		        config.put("workerIndex", String.valueOf(j++));
				if (MasterServlet.sendJob(dest, "POST", config, "definejob", mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job)).getResponseCode() != HttpURLConnection.HTTP_OK) {					
					throw new RuntimeException("Job definition request failed");
				}
			}
			for (String dest: workersList) {
				if (MasterServlet.sendJob(dest, "POST", config, "runjob", "").getResponseCode() != HttpURLConnection.HTTP_OK) {						
					throw new RuntimeException("Job execution request failed");
				}
			}
		}		
		catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}
}
