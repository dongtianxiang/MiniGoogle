package edu.upenn.cis.stormlite.bolts;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.log4j.Logger;
import edu.upenn.cis.stormlite.infrastructure.Job;
import edu.upenn.cis.stormlite.infrastructure.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.infrastructure.TopologyContext;
import edu.upenn.cis.stormlite.infrastructure.WorkerHelper;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.database.DBInstance;
import edu.upenn.cis455.database.DBManager;

/**
 * A simple adapter that takes a MapReduce "Job" and calls the "reduce"
 * on a per-tuple basis
 * 
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class ReduceBolt implements IRichBolt {
	static Logger log = Logger.getLogger(ReduceBolt.class);

	private DBInstance db;
	private Job reduceJob;
	private double d;
	
	Map<String, String> config;

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordCounter, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();    
	Fields schema = new Fields("key", "value");
	boolean sentEof = false;
	/**
     * This is where we send our output stream
     */
    private OutputCollector collector;    
    int eosRequired;
    int count = 0;
    
    public ReduceBolt() { }
    
    /**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf, 
    		TopologyContext context, OutputCollector collector) {
    	
    	config = stormConf;
    	
    	DBManager.createDBInstance(stormConf.get("databaseDir"));
    	db = DBManager.getDBInstance(stormConf.get("databaseDir"));
    	d = Double.parseDouble(stormConf.get("decayFactor"));
    	
        this.collector = collector;
        if (!stormConf.containsKey("reduceClass"))
        	throw new RuntimeException("Mapper class is not specified as a config option");
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

		int numMappers = Integer.parseInt(stormConf.get("mapExecutors"));	
		int numSpouts = Integer.parseInt(stormConf.get("spoutExecutors"));	
		int numReducers = Integer.parseInt(stormConf.get("reduceExecutors"));
		String[] workers = WorkerHelper.getWorkers(stormConf);				
		int numWorkers = workers.length;
		int M = ((numWorkers - 1) * numMappers  + 1) * numSpouts;		
        eosRequired = M * numReducers * (numWorkers - 1) * numMappers + M * numMappers;
        log.info("Num EOS required for ReduceBolt: " + eosRequired);
    }

    /**
     * Process a tuple received from the stream, buffering by key
     * until we hit end of stream
     */
    @Override
    public synchronized void execute(Tuple input) {
    	
    	if (sentEof) {
    		
	        if (!input.isEndOfStream())
	        	throw new RuntimeException("We received data after we thought the stream had ended!");
			       	        
		} 
    	else if (input.isEndOfStream()) {
						
			log.info("EOS Recevied: " + ++count);			
			eosRequired--;						
			if (eosRequired == 0) {
				
				log.info("*** Reducer has received all expected EOSes! ***");	
				log.info("*** Start of Reducing phase! ***");
				
				
				config.put("status", "Reducing");
				
				Map<String, List<String>> table = db.getTable(executorId);				
				Iterator<String> keyIt = table.keySet().iterator();				
				while (keyIt.hasNext()) {		
					String key = keyIt.next();
					table.get(key).add((new Double(1 - d)).toString());
					reduceJob.reduce(key, table.get(key).iterator(), collector);
				}
				sentEof = true;
			}
			collector.emitEndOfStream();
			
    	} 
    	else {
    		
    		Integer written = Integer.parseInt(config.get("keysWritten"));
    		config.put("keysWritten", (new Integer(written + 1)).toString());
    		
    		String key = input.getStringByField("key");
	        String value = input.getStringByField("value");
	        log.info("Reduce bolt received: " + key + " / " + value);  
	        db.addValue(executorId, key, value);
	        db.synchronize();
    	}
    }

    /**
     * Shutdown, just frees memory
     */
    @Override
    public void cleanup() {
    }

    /**
     * Lets the downstream operators know our schema
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema);
    }

    /**
     * Used for debug purposes, shows our executor/operator's unique ID
     */
	@Override
	public String getExecutorId() {
		return executorId;
	}

	/**
	 * Called during topology setup, sets the router to the next
	 * bolt
	 */
	@Override
	public void setRouter(StreamRouter router) {
		this.collector.setRouter(router);
	}

	/**
	 * The fields (schema) of our output stream
	 */
	@Override
	public Fields getSchema() {
		return schema;
	}
}
