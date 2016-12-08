package edu.upenn.cis.stormlite.bolts.pagerank;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.bolts.IRichBolt;
import edu.upenn.cis.stormlite.bolts.OutputCollector;
import edu.upenn.cis.stormlite.infrastructure.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.infrastructure.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.database.DBInstance;
import edu.upenn.cis455.database.DBManager;
import edu.upenn.cis455.database.Node;

public class PRResultBolt implements IRichBolt {
	
	public static Logger log = Logger.getLogger(PRResultBolt.class);
	public String executorId = UUID.randomUUID().toString();
	public Fields schema     = new Fields();
	public int eosRequired = 0;
    public String outputDirectory;
    public File targetDirectory;
    public File targetFile;
    public PrintWriter pw;    
    public int eosReceived = 0;
    public static Map<String, String> config;    
    public String serverIndex;
    public DBInstance graphData;
    
	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public void cleanup() {

	}

	@Override
	public void execute(Tuple input) {
		
		if (!input.isEndOfStream()) {
			
			String key = input.getStringByField("key");
			String val = input.getStringByField("value");			
			String output = key + " -> " + val;	
			
			Node node = graphData.getNode(key);
			node.setRank(Double.parseDouble(val));
			graphData.addNode(node);
			
			pw.println(output);
			pw.flush();			
		}
		else {
			eosReceived++;
			if (eosRequired == eosReceived) {
				log.info("* Page Rank Iteration complete *");
				config.put("status", "IDLE");
				pw.close();			
			}
			log.debug("EOS Receved: " + eosReceived);
		}
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		
		int numMappers = Integer.parseInt(stormConf.get("mapExecutors"));	
		int numSpouts = Integer.parseInt(stormConf.get("spoutExecutors"));	
		int numReducers = Integer.parseInt(stormConf.get("reduceExecutors"));			
		int numWorkers = Integer.parseInt(stormConf.get("workers"));		
		int M = ((numWorkers - 1) * numMappers + 1) * numSpouts;		
        int N = M * numReducers * (numWorkers - 1) * numMappers + M * numMappers; 
        
        eosRequired = numWorkers * numReducers * N;
        log.info("Num EOS required for Result Bolt: " + this.eosRequired); 
        
		String graphDataDir = stormConf.get("graphDataDir");
		String serverIndex = stormConf.get("workerIndex");
		if (serverIndex != null) {
			graphDataDir += "/" + serverIndex;
//			log.info("***************" + graphDataDir + "******************");
		}
		graphData = DBManager.getDBInstance(graphDataDir);
        config = stormConf;        
        serverIndex = stormConf.get("workerIndex"); 
        String outputDir = stormConf.get("outputDir");
        String targetFileDirectory = outputDir;
        if (serverIndex != null) {
        	targetFileDirectory = outputDir + "." + serverIndex;
        }
        targetFile = new File(targetFileDirectory);
        try {
			pw = new PrintWriter(targetFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void setRouter(StreamRouter router) {
		// ignore
	}

	@Override
	public Fields getSchema() {
		return schema;
	}

}
