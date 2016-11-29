package edu.upenn.cis.stormlite.bolts;

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

/**
 * A simple adapter that takes a MapReduce "Job" and calls the "map"
 * on a per-tuple basis.
 * 
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

public class MapBolt implements IRichBolt {
	
	static Logger log = Logger.getLogger(MapBolt.class);
	Job mapJob;

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordCounter, aka each "executor"
     */
    public String executorId = UUID.randomUUID().toString();   
	public Fields schema = new Fields("key", "value");	
	public Map<String, String> config;
	
	/**
	 * This tracks how many "end of stream" messages we've seen
	 * The aforementioned end of stream marks should come from spout
	 */
	private int eosNeeded = 0;

	/**
     * This is where we send our output stream
     */
    private OutputCollector collector;
//    private TopologyContext context;
    
    public MapBolt() {
    	
    }
    
	/**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf, TopologyContext context, OutputCollector collector) {
    	
    	
        this.collector = collector;
        
        config = stormConf;        
        log.info(this.config);
        log.info("********** Start of mapping phase ********");       
        stormConf.put("status", "Mapping");
        
        if (!stormConf.containsKey("mapClass")) {
        	throw new RuntimeException("Mapper class is not specified as a config option");
        }
        else {
        	String mapperClass = stormConf.get("mapClass");        	
        	try {        		
				mapJob = (Job)Class.forName(mapperClass).newInstance();				
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {				
				e.printStackTrace();
				throw new RuntimeException("Unable to instantiate the class " + mapperClass);
			}
        }
        
        if (!stormConf.containsKey("spoutExecutors")) {
        	throw new RuntimeException("Mapper class doesn't know how many input spout executors");
        }
        
		int numMappers = Integer.parseInt(stormConf.get("mapExecutors"));	
		int numSpouts = Integer.parseInt(stormConf.get("spoutExecutors"));
		String[] workers = WorkerHelper.getWorkers(stormConf);		
		
		int numWorkers = workers.length;
        eosNeeded = ((numWorkers - 1) * numMappers  + 1) * numSpouts;	
        log.info("Num EOS required for MapBolt: " + eosNeeded);
    }

    /**
     * Process a tuple received from the stream, incrementing our
     * counter and outputting a result
     */
    @Override
    public synchronized void execute(Tuple input) {
    	
    	log.info("Received tuple: " + input.toString()); 	
    	if (!input.isEndOfStream()) {
    		
	        String key = input.getStringByField("key");	 
	        
//          TODO
//	        synchronized(WorkerServer.keysRead) {
//	        	++WorkerServer.keysRead;
//	        }
	        
	        
	        
	        String value = input.getStringByField("value");
	        log.debug(getExecutorId() + " received " + key + " / " + value);	 
	        
	        log.info("EOS Needed: " + eosNeeded);
	        
	        if (eosNeeded == 0) {
	        	return;
//	        	throw new RuntimeException("We received data after we thought the stream had ended!");
	        }
	        
//          TODO
//	        if (!WorkerServer.status.equals("Mapping")) {
//	        	WorkerServer.status = "Mapping";
//	        }
	        
	        mapJob.map(key, value, collector);
	        
    	} else if (input.isEndOfStream()) {
    		
    		eosNeeded--;    		
    		if (eosNeeded == 0) {
    			log.info("Mapping phase completed");
    			
//              TODO    		
//    			synchronized(WorkerServer.status) {
//    				WorkerServer.status = "waiting";
//    			}
    			
    		}
    		collector.emitEndOfStream();
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
