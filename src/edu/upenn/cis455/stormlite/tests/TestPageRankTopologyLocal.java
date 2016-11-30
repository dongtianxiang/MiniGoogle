package edu.upenn.cis455.stormlite.tests;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.bolts.PRMapBolt;
import edu.upenn.cis.stormlite.bolts.PRReduceBolt;
import edu.upenn.cis.stormlite.bolts.PRResultBolt;
import edu.upenn.cis.stormlite.infrastructure.Configuration;
import edu.upenn.cis.stormlite.infrastructure.LocalCluster;
import edu.upenn.cis.stormlite.infrastructure.Topology;
import edu.upenn.cis.stormlite.infrastructure.TopologyBuilder;
import edu.upenn.cis.stormlite.spouts.RankFileSpout;
import edu.upenn.cis.stormlite.spouts.RankSpout;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis455.database.DBManager;

public class TestPageRankTopologyLocal {
	
	static Logger log = Logger.getLogger(TestPageRankTopologyLocal.class);
	// update: now reading out-bound links from DB instead of the ranks file
	public static String RANKS_SPOUT  = "LINKS_SPOUT";
	public static String MAP_BOLT     = "MAP_BOLT";
	public static String REDUCE_BOLT  = "REDUCE_BOLT";
	public static String RESULT_BOLT  = "RESULT_BOLT";
	
	public static void main(String[] args) throws Exception {
		
		Properties props = new Properties();
		props.load(new FileInputStream("./resources/log4j.properties"));
		PropertyConfigurator.configure(props);
		
		String jobClass = "edu.upenn.cis455.mapreduce.jobs.PageRankJob";
		int numMappers   = 2;
		int numReducers  = 2;
		String inputDir  = "data1";
		String outputDir = "out1";
		String jobName   = "Job1";
		
		RankFileSpout  spout = new RankSpout();
		PRMapBolt    mapper  = new PRMapBolt();
		PRReduceBolt reducer = new PRReduceBolt();
		PRResultBolt result  = new PRResultBolt();
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(RANKS_SPOUT,  spout,  1);
		builder.setBolt(MAP_BOLT,     mapper,  2).fieldsGrouping(RANKS_SPOUT, new Fields("key"));
		builder.setBolt(REDUCE_BOLT, reducer,  2).fieldsGrouping(MAP_BOLT,    new Fields("value"));
		builder.setBolt(RESULT_BOLT,  result,  1).firstGrouping(REDUCE_BOLT);
		
		Configuration config  = new Configuration();
		LocalCluster  cluster = new LocalCluster();
		Topology      topo    = builder.createTopology();
		ObjectMapper  om      = new ObjectMapper();
		DBManager.createDBInstance("storage");
		
        config.put("mapClass", jobClass);
        config.put("reduceClass", jobClass);
        config.put("spoutExecutors", "1");
        config.put("mapExecutors",    "" + numMappers);
        config.put("reduceExecutors", "" + numReducers);
        config.put("inputDir",  inputDir);
        config.put("outputDir", outputDir);
        config.put("job", jobName);
        config.put("databaseDir", "storage");
        config.put("workers", "1");       
        config.put("decayFactor", "0.85");
        config.put("graphDataDir", "graphStore");
        
		try {
			String str = om.writeValueAsString(topo);
			System.out.println("The StormLite topology is:\n" + str);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		
		// one iteration
		cluster.submitTopology("test", config, topo);
		 
        Thread.sleep(30000);
        cluster.killTopology("test");
        cluster.shutdown();
        System.exit(0);
	}
	

}
