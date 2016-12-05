package edu.upenn.cis.stormlite.bolts.BuildGraph;

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
	public Map<String, String> config;
    public String executorId = UUID.randomUUID().toString();
	public Fields schema = new Fields("key");
	public DBInstance graphDB;
	public DBInstance tempDB;
	public OutputCollector collector;
	public boolean sentEof = false;
	public String serverIndex;
	public File outfile;
	public FileWriter outputWriter;
	public int eosNeeded;
	
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
		
    	if (sentEof) {   		
	        if (!input.isEndOfStream()) {
	        	throw new RuntimeException("We received data after we thought the stream had ended!");
	        }
		}
    	else if (input.isEndOfStream()) {
    		
	        eosNeeded--;
	        if (eosNeeded == 0) {        	
	        	Map<String, List<String>> table = tempDB.getTable(executorId);		        	
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
		        		graphDB.addNode(node);	
		        		try {
							outputWriter.write(String.format("%s\n", node.getID()));
							outputWriter.flush();
						} catch (IOException e) {
							e.printStackTrace();
						}
		        		
	        		}
	        		
	        	}
		        log.info("************** Secondary reducer job completed! ******************");
	        }
	        
    	}
    	else {
    		
    		String key = input.getStringByField("value");
    		String value = input.getStringByField("key");
	        log.info("Secondary reducer received: " + key); 	        
	        tempDB.addKeyValue(executorId, key, value);
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
		
		if (!stormConf.containsKey("workers")) 
			throw new RuntimeException("Secondary reducer does not know the number of worker servers");
		
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
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		graphDB = DBManager.getDBInstance(graphDataDir);		
		DBManager.createDBInstance(databaseDir);
		tempDB  = DBManager.getDBInstance(databaseDir);
		
		eosNeeded = Integer.parseInt(numWorkers);
		log.info("Number of EOS needed for secondary reducers");
			
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
