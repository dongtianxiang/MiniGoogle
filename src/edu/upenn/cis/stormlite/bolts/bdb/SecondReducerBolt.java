package edu.upenn.cis.stormlite.bolts.bdb;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
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

public class SecondReducerBolt implements IRichBolt {
	
	public static Logger log = Logger.getLogger(SecondReducerBolt.class);
	public static Map<String, String> config;
	public static DBInstance graphDB;
	public static DBInstance tempDB;
	
    public String executorId = UUID.randomUUID().toString();
	public Fields schema = new Fields("key");
	public OutputCollector collector;
	public boolean sentEOS = false;
	public String serverIndex;
	public File outfile;
	public FileWriter outputWriter;
	public int eosREQUIRED;
	
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
		
    	if (sentEOS) {   		
	        if (!input.isEndOfStream()) {
	        	throw new RuntimeException("We received data after we thought the stream had ended!");
	        }
		}
    	else if (input.isEndOfStream()) {
    		
	        eosREQUIRED -= 1;
	        if (eosREQUIRED == 0) {
	        	
	        	log.info("start second level reduction");	   
	        	Map<String, List<String>> table;
	        	synchronized(tempDB) {
	        		table = tempDB.getTable(executorId);
	        	}
	        	
	        	log.info(table);
	        	
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
		        		try {
							outputWriter.write(String.format("%s\n", node.getID()));
							outputWriter.flush();
						} catch (IOException e) {
							e.printStackTrace();
						}
		        		graphDB.addNode(node);	
	        		}   		
	        	}
	        	
	        	synchronized(tempDB) {
	        		tempDB.clearTempData();
	        	}
	        	
	        	config.put("status", "IDLE");	        	
//		        log.info("************** Secondary reducer job completed! ******************");
	        }
	        
    	}
    	else {   		
    		
    		if (eosREQUIRED == 0) {
    			throw new IllegalStateException();
    		}
    		
    		String key = input.getStringByField("value");
    		String value = input.getStringByField("key");
	        log.info("Server#" + serverIndex + ": Secondary reducer received: " + value + " -> " + key); 	
	        
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
		
		try {
			outputWriter = new FileWriter(outfile, true);
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		
		// if database instances do not exist, they should
		// be automatically created
		graphDB = DBManager.getDBInstance(graphDataDir);
		tempDB  = DBManager.getDBInstance(databaseDir);
		
		eosREQUIRED = Integer.parseInt(numWorkers);			
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