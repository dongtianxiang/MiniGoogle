package edu.upenn.cis455.stormlite.index.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Arrays;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.bolts.bdb.BuilderMapBolt;
import edu.upenn.cis.stormlite.bolts.bdb.FirstaryReduceBolt;
import edu.upenn.cis.stormlite.bolts.bdb.SecondReducerBolt;
import edu.upenn.cis.stormlite.infrastructure.Configuration;
import edu.upenn.cis.stormlite.infrastructure.Topology;
import edu.upenn.cis.stormlite.infrastructure.TopologyBuilder;
import edu.upenn.cis.stormlite.infrastructure.WorkerJob;
import edu.upenn.cis.stormlite.spouts.bdb.LinksFileSpout;
import edu.upenn.cis.stormlite.spouts.bdb.LinksSpout;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis455.mapreduce.servlets.MasterServlet;

public class TestStorm {

	private static final String SPOUT = "INDEXER_SPOUT";
	private static final String MAP_BOLT = "MAP_BOLT";
	private static final String REDUCE_BOLT = "REDUCE_BOLT";
	private static final String REDUCE_BOLT_2 = "REDUCE_BOLT_2";

	public static void main(String[] args) throws IOException, InterruptedException {
		int numMappers = 2;
		int numReducers = 2;
		int numSpouts = 1;
		int numReducers2 = 2;

		String inputDBDir = "./dtianx";
		String outputDir = "indexerResults";
		String jobName = "indexer";

		
		/*SET AWS KEY ID*/
		FileReader f = null;
		try {
			f = new FileReader(new File("./conf/config.properties"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		BufferedReader bf = new BufferedReader(f);
		System.setProperty("KEY", bf.readLine());
		System.setProperty("ID", bf.readLine());
		
		
		
		LinksFileSpout spout = new LinksSpout();
		BuilderMapBolt mapBolt = new BuilderMapBolt();
		FirstaryReduceBolt reduceBolt = new FirstaryReduceBolt();
		SecondReducerBolt postBolt = new SecondReducerBolt();

		// build topology
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(SPOUT, spout, numSpouts);
		builder.setBolt(MAP_BOLT, mapBolt, numMappers).shuffleGrouping(SPOUT);
		builder.setBolt(REDUCE_BOLT, reduceBolt, numReducers).fieldsGrouping(MAP_BOLT, new Fields("word"));
		builder.setBolt(REDUCE_BOLT_2, postBolt, numReducers2).fieldsGrouping(REDUCE_BOLT, new Fields("url"));

		Topology topo = builder.createTopology();

		// create configuration object
		Configuration config = new Configuration();
		config.put("spoutExecutors", (new Integer(numSpouts)).toString());
		config.put("mapExecutors", (new Integer(numMappers)).toString());
		config.put("reduceExecutors", (new Integer(numReducers)).toString());
		config.put("reduce2Executors", (new Integer(numReducers2)).toString());
		config.put("inputDBDir", inputDBDir);
		config.put("outputDir", outputDir);
		config.put("job", jobName);
		config.put("workers", "2");
		/*
		//TODO
		config.put("graphDataDir", "graphStore");
		config.put("databaseDir", "storage");
		
		*/
		
		config.put("status", "IDLE");
		config.put("numThreads", "10");

		WorkerJob job = new WorkerJob(topo, config);
		ObjectMapper mapper = new ObjectMapper();
		mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
		String[] workersList = new String[] { "127.0.0.1:8000", "127.0.0.1:8001" };
		config.put("workerList", Arrays.toString(workersList));

		try {
			int j = 0;
			for (String dest : workersList) {
				config.put("workerIndex", String.valueOf(j++));
				if (MasterServlet
						.sendJob(dest, "POST", config, "definejob",
								mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job))
						.getResponseCode() != HttpURLConnection.HTTP_OK) {
					throw new RuntimeException("Job definition request failed");
				}
			}
			for (String dest : workersList) {
				if (MasterServlet.sendJob(dest, "POST", config, "runjob", "")
						.getResponseCode() != HttpURLConnection.HTTP_OK) {
					throw new RuntimeException("Job execution request failed");
				}
			}
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		Thread.sleep(5000);
		System.exit(0);
	}

}
