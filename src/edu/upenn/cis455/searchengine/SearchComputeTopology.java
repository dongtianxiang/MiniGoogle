package edu.upenn.cis455.searchengine;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.infrastructure.Configuration;
import edu.upenn.cis.stormlite.infrastructure.LocalCluster;
import edu.upenn.cis.stormlite.infrastructure.Topology;
import edu.upenn.cis.stormlite.infrastructure.TopologyBuilder;
import edu.upenn.cis.stormlite.tuple.Fields;

public class SearchComputeTopology {	
	
	static Logger log = Logger.getLogger(SearchComputeTopology.class);
	public static String SPOUT  = "SPOUT";
	public static String MAP_BOLT     = "MAP_BOLT";
	public static String REDUCE_BOLT  = "REDUCE_BOLT";
	public static String RESULT_BOLT  = "RESULT_BOLT";
		
	public static void runMapreduce() throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException {
		// log4j set up
		Properties props = new Properties();
		props.load(new FileInputStream("./resources/log4j.properties"));
		PropertyConfigurator.configure(props);
		
		String jobClass = "edu.upenn.cis455.mapreduce.jobs.SearchJob";
		int numMappers   = 2;
		int numReducers  = 2;
		String inputDir  = ".";
		String outputDir = "search";
		String jobName   = "SearchJob";
		
		SearchSpout  spout = new SearchSpout();
		SearchMapBolt    mapper  = new SearchMapBolt();
		SearchReduceBolt reducer = new SearchReduceBolt();
		SearchResultBolt result  = new SearchResultBolt();
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(SPOUT,  spout,  1);
		builder.setBolt(MAP_BOLT,     mapper,  2).shuffleGrouping(SPOUT);
		builder.setBolt(REDUCE_BOLT, reducer,  2).fieldsGrouping(MAP_BOLT,    new Fields("key"));
		builder.setBolt(RESULT_BOLT,  result,  1).firstGrouping(REDUCE_BOLT);
		
		Configuration config  = new Configuration();
		LocalCluster  cluster = new LocalCluster();
		Topology      topo    = builder.createTopology();
		ObjectMapper  om      = new ObjectMapper();
		
        config.put("mapClass", jobClass);
        config.put("reduceClass", jobClass);
        config.put("spoutExecutors", "1");
        config.put("mapExecutors",    "" + numMappers);
        config.put("reduceExecutors", "" + numReducers);
        config.put("inputDir",  inputDir);
        config.put("job", jobName);
        config.put("workers", "1");       
        
		try {
			String str = om.writeValueAsString(topo);
			log.info("The StormLite topology is:\n" + str);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}		
		cluster.submitTopology("test", config, topo);
		 
        Thread.sleep(30000);
        cluster.killTopology("test");
        cluster.shutdown();
        System.exit(0);
	}
}
