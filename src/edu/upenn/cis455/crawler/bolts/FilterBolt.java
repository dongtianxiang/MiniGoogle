package edu.upenn.cis455.crawler.bolts;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.infrastructure.*;
import edu.upenn.cis.stormlite.bolts.IRichBolt;
import edu.upenn.cis.stormlite.bolts.OutputCollector;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.PageDownloader;
import edu.upenn.cis455.crawler.RobotCache;
import edu.upenn.cis455.crawler.URLFrontierQueue;
import edu.upenn.cis455.crawler.XPathCrawler;

/**
 * Bolt Component used to filter coming extracted urls
 * @author cis555
 *
 */
public class FilterBolt implements IRichBolt{
	static Logger log = Logger.getLogger(FilterBolt.class);
	
	Fields schema = new Fields("extractedLink");
	
	String executorId = UUID.randomUUID().toString();
	
	private OutputCollector collector;
	
	private URLFrontierQueue urlQueue;
	
    public FilterBolt() {
    	log.debug("Starting FilterBolt");
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
	
	private String removeHashTagInURL(String link) {
		String[] split = link.split("#");
		return split[0];
	}
	
    /**
     * Process a tuple received from the stream, incrementing our
     * counter and outputting a result
     */
	@Override
	public void execute(Tuple input) {
		long start = System.currentTimeMillis();
		Queue<String> linksToCheck = (Queue<String>) input.getObjectByField("URLStream");
		String url = input.getStringByField("url");
		long step1 = System.currentTimeMillis();
		urlQueue.setOutLinks(url, linksToCheck);
		long step2 = System.currentTimeMillis();
		log.info(url + " <----> Outlinks Recorded " + "step1: " + (step1-start) + "ms " + "step2: " + (step2-step1) + "ms");	
		
		long max = 0;
		long step3 = 0;
		long step4 = 0;
		while(!linksToCheck.isEmpty()) {
			step3 = System.currentTimeMillis();
			if(step3 - start > 15000) {
				log.info(url + " -->  costs so much time, break out loop Mandatorily");
				break;
			}
			
			String link = linksToCheck.poll();
			link = removeHashTagInURL(link);
			if(!RobotCache.checkDelay(link)) {       
				// linksToCheck.offer(link);    //  Once delay detected, avoid this host for better crawling performance.
				log.debug(link + " ******* Delayed");
				continue;
			}
			collector.emit(new Values<Object>(link));
			
			step4 = System.currentTimeMillis();
			if(step4 - step3 > max) max = step4 - step3; 
		}
		long end = System.currentTimeMillis();
		log.info(url + " <----> Emit Finished, longest Time: " + max + "ms " + " whole FilterBolt: " + (end - start) + " ms");	
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
