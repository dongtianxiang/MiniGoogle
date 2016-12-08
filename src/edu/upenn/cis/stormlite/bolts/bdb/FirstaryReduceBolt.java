package edu.upenn.cis.stormlite.bolts.bdb;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.bolts.IRichBolt;
import edu.upenn.cis.stormlite.bolts.OutputCollector;
import edu.upenn.cis.stormlite.infrastructure.Job;
import edu.upenn.cis.stormlite.infrastructure.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.infrastructure.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.database.DBInstance;
import edu.upenn.cis455.database.DBManager;
import edu.upenn.cis455.database.Node;

public class FirstaryReduceBolt implements IRichBolt {
	
	public static Logger log = Logger.getLogger(FirstaryReduceBolt.class);
	public static Map<String, String> config;
    public String executorId = UUID.randomUUID().toString();
	public Fields schema = new Fields("key", "value"); 
	public Job reduceJob;
	public OutputCollector collector;
	public Integer eosNeeded = 0;
	public static DBInstance graphDB;
	public static DBInstance tempDB;
	public boolean sentEof = false;
	public int count = 0;
	public double d;
	public String serverIndex;
	public File outfile;
	public FileWriter outputWriter;
	public static AtomicBoolean eosSent;
	
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
    		
//			log.debug("EOS Received: " + (++count));			
			eosNeeded--;
			if (eosNeeded == 0) {
				
				eosSent.set(true);
				
				log.info("start first-level reduction");				
				config.put("status", "REDUCING");				
				Map<String, List<String>> table;
				
				synchronized(tempDB) {
					table = tempDB.getTable(executorId);	
				}
				
				log.info("Server# " + serverIndex + " " + table);				
				Iterator<String> keyIt = table.keySet().iterator();	
				
				while (keyIt.hasNext()) {
					String key = keyIt.next();					
					Node node;
					Iterator<String> valueIt = table.get(key).iterator();					
			        if (!graphDB.hasNode(key)) {			        	
			        	node  = new Node(key);		
			        	try {
							outputWriter.write(String.format("%s\n", key));
				        	outputWriter.flush();
						} catch (IOException e) {
							e.printStackTrace();
						}
			        }
			        else {
			        	node = graphDB.getNode(key);
			        }					
					while (valueIt.hasNext()) {
						String nextVal = valueIt.next();
						node.addNeighbor(nextVal);
						collector.emit(new Values<Object>(key, nextVal));
					}
		        	graphDB.addNode(node);
				}
				
				try {
					outputWriter.close();
				} catch (IOException e) {
					e.printStackTrace();
				}			
				synchronized(tempDB) {
					tempDB.clearTempData();	
				}
				
				
				
				collector.emitEndOfStream();
			}
    	}
    	else {
    		
    		if (eosSent.get()) {
    			
    			throw new IllegalStateException("We received data after we think the stream has ended!");
    		}
    		
    		
    		String key = input.getStringByField("key");
	        String value = input.getStringByField("value");	        	              
	        log.info("Server# " + serverIndex + " Firstary reducer received: " + key + " / " + value);
	        synchronized(tempDB) {
	        	tempDB.addKeyValue(executorId, key, value);
	        }
    	}		
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
				
		config = stormConf;
		serverIndex = stormConf.get("workerIndex");
		
		eosSent = new AtomicBoolean(false);
		
		String graphDataDir  = config.get("graphDataDir");
		String databaseDir   = config.get("databaseDir");
		String outputFileDir = config.get("outputDir");	
		
		if (serverIndex != null) {
			graphDataDir  += "/" + serverIndex;
			databaseDir   += "/" + serverIndex + "-1";
			outputFileDir += "/" + serverIndex;
		}
				
		File outfileDir = new File(outputFileDir);
		outfileDir.mkdirs();
		
		String outputFileName = "names.txt";
		outfile = new File(outfileDir, outputFileName);
		
		try {
			outputWriter = new FileWriter(outfile, false);
		}
		catch (IOException e) {
			e.printStackTrace();
			return;
		}
		
		graphDB = DBManager.getDBInstance(graphDataDir);		
		DBManager.createDBInstance(databaseDir);
		tempDB  = DBManager.getDBInstance(databaseDir);
			
        this.collector = collector;
        if (!stormConf.containsKey("mapExecutors")) {
        	throw new RuntimeException("Reducer class doesn't know how many map bolt executors");
        }

//		int numMappers  = Integer.parseInt(stormConf.get("mapExecutors"));	
//		int numSpouts   = Integer.parseInt(stormConf.get("spoutExecutors"));	
//		int numReducers = Integer.parseInt(stormConf.get("reduceExecutors"));
		int numWorkers  = Integer.parseInt(stormConf.get("workers"));	
//		int M = ((numWorkers - 1) * numMappers  + 1) * numSpouts;		
//        eosNeeded = M * numReducers * (numWorkers - 1) * numMappers + M * numMappers;		
		eosNeeded = numWorkers;	
        log.info("Num EOS required for ReduceBolt: " + eosNeeded);
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
