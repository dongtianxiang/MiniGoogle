package edu.upenn.cis455.crawler.bolts;

import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.infrastructure.*;
import edu.upenn.cis.stormlite.bolts.IRichBolt;
import edu.upenn.cis.stormlite.bolts.OutputCollector;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.crawler.RobotCache;
import edu.upenn.cis455.crawler.URLFrontierQueue;
import edu.upenn.cis455.crawler.XPathCrawler;

public class RecordBolt implements IRichBolt{
	static Logger log = Logger.getLogger(RecordBolt.class);
	
	String executorId = UUID.randomUUID().toString();
	Fields schema = new Fields();
	private OutputCollector collector;
	private URLFrontierQueue urlQueue;
	
    public RecordBolt() {
    	log.debug("Starting RecordBolt");
    	this.urlQueue = XPathCrawler.urlQueue;
    }
    
    
    /**
     * Used for debug purposes, shows our exeuctor/operator's unique ID
     */
	@Override
	public String getExecutorId() {
		return executorId;
	}
	
    /**
     * Lets the downstream operators know our schema
     */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(schema);
	}
	
    /**
     * Shutdown, just frees memory
     */
	@Override
	public void cleanup() {}
	
    /**
     * Process a tuple received from the stream, incrementing our
     * counter and outputting a result
     */
	@Override
	public void execute(Tuple input) {
		long start = System.currentTimeMillis();
		String link = input.getStringByField("extractedLink");
		long step1 = System.currentTimeMillis();
		if(RobotCache.isValid(link)) {     // Check the robots.txt allowing list
			long step2 = System.currentTimeMillis();
//			RobotCache.setCurrentTime(link);  
//			long step3 = System.currentTimeMillis();
//			if(!urlQueue.filter(link)) return; // HEAD REQUEST set the last visited time
			long step4 = System.currentTimeMillis();
			urlQueue.pushURL(link);
			long step5 = System.currentTimeMillis();
			//urlQueue.putIntoVisitedURL(link, RobotCache.getLastVisited(link));
			log.info(link + " --> pushed into queue" + " step1: " + (step1-start) + "ms "
					+ "step2: " + (step2-step1) + "ms " + "step5: " + (step5-step4) + "ms");
		} else {
			log.debug(link + " --> Not Valid on this Host");
		}
	}
	
    /**
     * Initialization, just saves the output stream destination
     */
	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
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
