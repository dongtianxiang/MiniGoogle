package edu.upenn.cis.stormlite.bolts.PageRank;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.bolts.IRichBolt;
import edu.upenn.cis.stormlite.bolts.OutputCollector;
import edu.upenn.cis.stormlite.infrastructure.Job;
import edu.upenn.cis.stormlite.infrastructure.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.infrastructure.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.database.DBInstance;
import edu.upenn.cis455.database.DBManager;

public class PRReduceBolt implements IRichBolt {
	
	public static Logger log = Logger.getLogger(PRReduceBolt.class);
	public Map<String, String> config;
    public String executorId = UUID.randomUUID().toString();
	public Fields schema = new Fields("key", "value"); 
	public Job reduceJob;
	public OutputCollector collector;
	public Integer eosNeeded = 0;
	public DBInstance graphDB;
	public DBInstance tempDB;
	public boolean sentEof = false;
	public int count = 0;
	public double d;
	public String serverIndex;
	
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
    		
			log.info("EOS Received: " + (++count));			
			eosNeeded--;
			if (eosNeeded == 0) {
				log.info("*** Reducer has received all expected End of Stream marks! ***");	
				log.info("***                Start of Reducing phase!                ***");
				Map<String, List<String>> table = tempDB.getTable(executorId);				
				Iterator<String> keyIt = table.keySet().iterator();	
				while (keyIt.hasNext()) {
					String key = keyIt.next();
					table.get(key).add((new Double(1 - d)).toString());
					reduceJob.reduce(key, table.get(key).iterator(), collector);
				}
				tempDB.clearTempData();
				log.info("Database instance has been reset.");			
			}
			collector.emitEndOfStream();
    	}
    	else {
    		String key = input.getStringByField("key");
	        String value = input.getStringByField("value");       
	        Double realVal = Double.parseDouble(value) * d;
	        
	        int written = Integer.parseInt(config.get("keysWritten"));
	        config.put("keysWritten", (new Integer(written + 1)).toString());
	        
	        log.info("Reduce bolt received: " + key + " / " + value);  
	        tempDB.addValue(executorId, key, (new Double(realVal * d)).toString());
	        tempDB.synchronize();
    	}		
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		
		
		config = stormConf;
		d = Double.parseDouble(config.get("decayFactor"));		
		serverIndex = stormConf.get("workerIndex");
		
		String graphDataDir = config.get("graphDataDir");
		String databaseDir  = config.get("databaseDir");
		
		if (serverIndex != null) {
			graphDataDir += "." + serverIndex;
			databaseDir  += "/" + serverIndex;
		}
		
		graphDB = DBManager.getDBInstance(graphDataDir);		
		DBManager.createDBInstance(databaseDir);
		tempDB  = DBManager.getDBInstance(databaseDir);
			
        this.collector = collector;
        if (!stormConf.containsKey("reduceClass")) {
        	throw new RuntimeException("Mapper class is not specified as a config option");
        }
        else {        	
        	String mapperClass = stormConf.get("reduceClass");        	
        	try {
				reduceJob = (Job)Class.forName(mapperClass).newInstance();
			} 
        	catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				e.printStackTrace();
				throw new RuntimeException("Unable to instantiate the class " + mapperClass);
			}
        }
        
        if (!stormConf.containsKey("mapExecutors")) {
        	throw new RuntimeException("Reducer class doesn't know how many map bolt executors");
        }

		int numMappers  = Integer.parseInt(stormConf.get("mapExecutors"));	
		int numSpouts   = Integer.parseInt(stormConf.get("spoutExecutors"));	
		int numReducers = Integer.parseInt(stormConf.get("reduceExecutors"));
		int numWorkers  = Integer.parseInt(stormConf.get("workers"));
		
		int M = ((numWorkers - 1) * numMappers  + 1) * numSpouts;		
        eosNeeded = M * numReducers * (numWorkers - 1) * numMappers + M * numMappers;
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
