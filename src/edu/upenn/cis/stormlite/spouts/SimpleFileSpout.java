package edu.upenn.cis.stormlite.spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.infrastructure.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.infrastructure.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.spouts.IRichSpout;
import edu.upenn.cis.stormlite.spouts.SpoutOutputCollector;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;

/**
 * Simple word spout, largely derived from
 * https://github.com/apache/storm/tree/master/examples/storm-mongodb-examples
 * but customized to use a file called words.txt.
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
public abstract class SimpleFileSpout implements IRichSpout {
	static Logger log = Logger.getLogger(SimpleFileSpout.class);

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordSpout, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();

    /**
	 * The collector is the destination for tuples; you "emit" tuples there
	 */
	SpoutOutputCollector collector;
	
	/**
	 * This is a simple file reader
	 */
	String filename;
    BufferedReader reader;
	Random r = new Random();
	
	@SuppressWarnings("rawtypes")
	Map config;
	
	int inx = 0;
	boolean sentEof = false;
	
    public SimpleFileSpout() {
    	filename = getFilename();
    }
    
    public abstract String getFilename();


    /**
     * Initializes the instance of the spout (note that there can be multiple
     * objects instantiated)
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        
        config = conf;
        config.put("status", "idle");
        
        try {
        	        	
        	log.debug("Starting spout for " + filename);
        	log.debug(getExecutorId() + " opening file reader");        	
        	String rootDir = (String)conf.get("inputDir");        	
        	log.info("Root directory specified at " + rootDir);        
        	
        	if (!rootDir.equals("")) {
        		rootDir += "/";
        	}
        	
        	StringBuilder targetPathBuilder = new StringBuilder(rootDir + filename);
        	// If we have a worker index, read appropriate file among xyz.txt.0, xyz.txt.1, etc.
        	if (conf.containsKey("workerIndex"))
        		// reader = new BufferedReader(new FileReader(filename + "." + conf.get("workerIndex")));
        		targetPathBuilder.append('.').append(conf.get("workerIndex"));
        	

        	reader = new BufferedReader(new FileReader(targetPathBuilder.toString()));
		} catch (FileNotFoundException e) {

			e.printStackTrace();
		}
    }

    /**
     * Shut down the spout
     */
    @Override
    public void close() {
    	if (reader != null)
	    	try {
				reader.close();
			} catch (IOException e) {

				e.printStackTrace();
			}
    }

    /**
     * The real work happens here, in incremental fashion.  We process and output
     * the next item(s).  They get fed to the collector, which routes them
     * to targets
     */
    @Override
    public synchronized void nextTuple() {
    	if (reader != null && !sentEof) {
	    	try {
		    	String line = reader.readLine();
		    	if (line != null) {
		    				    		
		        	log.debug(getExecutorId() + " read from file " + getFilename() + ": " + line);
		    		String[] words = line.split("[ \\t\\,.]");		
		    		for (String word: words) {
		            	log.debug(getExecutorId() + " emitting " + word);
		    	        this.collector.emit(new Values<Object>(String.valueOf(inx++), word));
		    		}
		    		
		    	} else if (!sentEof) {
		        	log.info("Finished reading file " + getFilename() + " and emitting EOS");
	    	        this.collector.emitEndOfStream();
	    	        sentEof = true;
		    	}
	    	} catch (IOException e) {
	    		e.printStackTrace();
	    	}
    	}
        Thread.yield();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "value"));
    }


	@Override
	public String getExecutorId() {
		
		return executorId;
	}


	@Override
	public void setRouter(StreamRouter router) {
		this.collector.setRouter(router);
	}

}
