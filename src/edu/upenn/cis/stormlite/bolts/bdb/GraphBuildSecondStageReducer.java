package edu.upenn.cis.stormlite.bolts.bdb;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

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
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
public class GraphBuildSecondStageReducer implements IRichBolt {
	
	public static Logger log = Logger.getLogger(GraphBuildSecondStageReducer.class);
//	Logger log = LoggerFactory.getLogger(SecondReducerBolt.class);
	public static Map<String, String> config;
	public static DBInstance graphDB;
	public static DBInstance tempDB;
	public int count = 0;
    public String executorId = UUID.randomUUID().toString();
	public Fields schema = new Fields("key");
	public OutputCollector collector;
	public boolean sentEOS = false;
	public String serverIndex;
	public File outfile;
	public File linksfile;
	public int eosREQUIRED;
	private FileWriterQueue fwq;
	private FileWriterQueue fwq2;
	public static AtomicBoolean eosSent = new AtomicBoolean();
	
	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(schema);
	}

	@Override
	public void cleanup() {
		
	}

	@Override
	public void execute(Tuple input) {
		
    	if (eosSent.get()) {      		
	        if (!input.isEndOfStream()) {
	        	log.error("Server# " + serverIndex + "::"+executorId+" Firstary MAYDAY MAYDAY! " + input.getStringByField("key") + " / " + input.getStringByField("value"));
	        	log.error("We received data after we thought the stream had ended!");
	        	return;
	        }
	        log.error("Server# " + serverIndex + "::"+executorId+" Firstary MAYDAY MAYDAY! EOS AGAIN!!!!!");
		}
    	else if (input.isEndOfStream()) {
    		eosREQUIRED -= 1;
    		log.info("Server#" + serverIndex + "::"+executorId+"EOS Received(reducer2): " + (++count)+"/"+(eosREQUIRED+count));			
	        
	        if (eosREQUIRED == 0) {
	        	eosSent.set(true);
	        	log.info("-- Start second stage reduction --");	
	        	config.put("status", "REDUCING2");
	        	Map<String, List<String>> table;
	        	synchronized(tempDB) {
	        		table = tempDB.getTable(executorId);
	        	}
	        	
	        	Iterator<String> iter = table.keySet().iterator();
	        	while (iter.hasNext()) {
	        		
	        		String dest = iter.next();        		
	        		if (!graphDB.hasNode(dest)) {
	        			        			
		        		Node node = new Node(dest); 		
		        		List<String> ancestors = table.get(dest);	
		        		node.addNeighbor(node.getID());
		        		for (String ancestor: ancestors) {
		        			node.addNeighbor(ancestor);
		        		}
		        		fwq.addQueue(String.format("%s\n", node.getID()));
	    	        	log.info(String.format("Server#" + serverIndex + " added %s", ancestors + " <- " + node.getID()));
		        		graphDB.addNode(node);	
	        		}   		
	        	}	      
	        	
	        	log.info("-- Second stage reduction complete --");	
	        	tempDB.clearTempData();	        	
	        	if(FileWriterQueue.getFileWriterQueueLaterCall().queue.isEmpty()) {
	        		config.put("status", "IDLE");	  
	        	}
				log.info("-- MR job complete --");	
	        }	        
    	}
    	else {   		    		
    		if (eosREQUIRED == 0) {
    			log.error("We received data after we thought the stream had ended!");
    			return;
    		}   		
    		String key = input.getStringByField("value");
    		String value = input.getStringByField("key");
	        log.debug("Server# " + serverIndex + ":: Secondary reducer received: " + value + " -> " + key); 	
	        synchronized(tempDB) {
	        	tempDB.addKeyValue(executorId, key, value);
	        }
    	}
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
				
		config = stormConf;
		serverIndex = stormConf.get("workerIndex");		
		String graphDataDir = config.get("graphDataDir");
		String outputFileDir = config.get("outputDir");	
		String databaseDir   = config.get("databaseDir");
		String numWorkers = config.get("workers");
		
		if (!stormConf.containsKey("workers")) {
			throw new RuntimeException("Secondary reducer does not know the number of worker servers");
		}
			
        if (!stormConf.containsKey("mapExecutors")) {
        	throw new RuntimeException("Reducer class doesn't know how many map bolt executors");
        }
		
		if (!stormConf.containsKey("reduceExecutors")) {		
			throw new RuntimeException("Secondary reducer need to know the number of first-level reducers.");
		}
		
		if (serverIndex != null) {
			graphDataDir  += "/" + serverIndex;
			outputFileDir += "/" + serverIndex;
			databaseDir   += "/" + serverIndex + "-2";
		}
		
		File outfileDir = new File(outputFileDir);
		if (!outfileDir.exists()) {
			outfileDir.mkdirs();
		}
		
		String outputFileName = "names.txt";
		outfile = new File(outfileDir, outputFileName);
		linksfile = new File(outfileDir, "links.txt");
		
		fwq = FileWriterQueue.getFileWriterQueue(outfile, context);
		
		// if database instances do not exist, they should
		// be automatically created
		graphDB = DBManager.getDBInstance(graphDataDir);
		tempDB  = DBManager.getDBInstance(databaseDir);
		
		int reducerNum = Integer.parseInt(stormConf.get("reduceExecutors"));
		int reducer2Num = Integer.parseInt(stormConf.get("reduce2Executors"));
		int workerNums = Integer.parseInt(numWorkers);	
		eosREQUIRED = (workerNums - 1) * reducerNum * reducer2Num + reducerNum;
        this.collector = collector;
	}

	@Override
	public void setRouter(StreamRouter router) {		
		// router has only to do with collector
		collector.setRouter(router);
	}

	@Override
	public Fields getSchema() {
		return schema;
	}

}
