package edu.upenn.cis.stormlite.spouts.LocalDBBuilder;

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

public abstract class LinksFileSpout implements IRichSpout {

	public static Logger log = Logger.getLogger(LinksFileSpout.class);
	public String executorId = UUID.randomUUID().toString();
	public String filename;
	public BufferedReader reader;
	public Random r = new Random();
	public SpoutOutputCollector collector;
	public Fields schema = new Fields("key", "value");	
	@SuppressWarnings("rawtypes")
	public Map config;
	public int inx = 0;
	public boolean eofSent = false;	
	public abstract String getFilename();

	public LinksFileSpout() {
		filename = getFilename();
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		
        this.collector = collector;
        config = conf;
        config.put("status", "IDLE");
        
        try {
        	
        	log.debug("Starting spout for " + filename);
        	log.debug(getExecutorId() + " opening file reader");        	
        	String rootDir = (String)conf.get("inputDir");        	
        	log.info("Root directory specified at " + rootDir);
        	
        	if (rootDir.length() != 0) {
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
	
	@Override
	public void close() {
		if (reader != null) {
			try {
				reader.close();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public synchronized void nextTuple() {
		
		if (reader != null && !eofSent) {
			try {
				String line = reader.readLine();
				if (line != null) {
					
					log.debug(getExecutorId() + " read from file " + getFilename() + ": " + line);					
					String[] parts = line.split(" -> ");
					String src  = parts[0];
					String links = parts[1];
					collector.emit(new Values<Object>(src, links));
				} 
				else if (!eofSent) {
					
					log.info("Finished reading file " + getFilename() + " and emitting EOS");
					collector.emitEndOfStream();
					eofSent = true;
				}				
			} 
			catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
		Thread.yield();
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {	
		declarer.declare(schema);
	}
	
	@Override
	public String getExecutorId() {
		return executorId;
	}
	
	@Override
	public void setRouter(StreamRouter router) {
		collector.setRouter(router);
	}
}
