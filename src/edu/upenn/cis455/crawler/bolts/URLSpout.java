package edu.upenn.cis455.crawler.bolts;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.infrastructure.*;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.spouts.IRichSpout;
import edu.upenn.cis.stormlite.spouts.SpoutOutputCollector;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.RobotCache;
import edu.upenn.cis455.crawler.URLFrontierQueue;
import edu.upenn.cis455.crawler.XPathCrawler;

/**
 * Main spout that checks delay and dequeue URLs from URLFrontier.
 * @author cis555
 *
 */
public class URLSpout implements IRichSpout{
	static Logger log = Logger.getLogger(URLSpout.class);
	URLFrontierQueue urlQueue;
	SpoutOutputCollector collector;
	int size = 0;
	
    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordSpout, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    
	public URLSpout(){
		log.debug("Starting Spout");
		this.urlQueue = XPathCrawler.urlQueue;
	}
	
	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url"));
	}

	@Override
	public void open(Map<String, String> config, TopologyContext topo, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void close() {}

	@Override
	public void nextTuple() {
		try {
			long start = System.currentTimeMillis(); 
			if(!urlQueue.isEmpty()) {
				String curURL = urlQueue.popURL();
				
				if(!RobotCache.checkDelay(curURL)) {
					urlQueue.pushURL(curURL);
				} else {				
					this.collector.emit(new Values<Object>(curURL));
					
					long end = System.currentTimeMillis(); 
					log.debug(curURL + "----> is spouted -> spout time: " + (end-start) + " ms" );
				}
			}
			Thread.yield();
		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			log.error(sw.toString()); // stack trace as a string
		} 
	}

	@Override
	public void setRouter(StreamRouter router) {
		this.collector.setRouter(router);
	}

}
