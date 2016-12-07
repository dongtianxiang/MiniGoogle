package edu.upenn.cis455.crawler.bolts;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

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
 * Bolt Component used to store document into database
 * @author cis555
 *
 */
public class DownloadBolt implements IRichBolt{
	static Logger log = Logger.getLogger(DownloadBolt.class);
	
	Fields schema = new Fields("url", "URLStream");
	
	String executorId = UUID.randomUUID().toString();
	
	private OutputCollector collector;
	
	private URLFrontierQueue urlQueue;
	
    public DownloadBolt() {
    	log.debug("Starting DownloadBolt");
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
		
		Document doc = (Document)input.getObjectByField("document");
		String type = input.getStringByField("type");
		String url = input.getStringByField("url");
		
		RobotCache.setCurrentTime(url);
		/* The Downloader itself updated the last visited time */
		PageDownloader.download(url, doc, type);
		
		urlQueue.putIntoVisitedURL(url, RobotCache.getLastVisited(url));
		int executedSize = urlQueue.addExecutedSize();
		log.info("----> " + url + ": Downloading");
		log.info("size: " + doc.toString().length());
		log.info(executedSize);
		
		Queue<String> linklist = new LinkedList<String>();
		Elements links = doc.select("a[href]");
		for (Element link : links) {
			linklist.add(link.attr("abs:href"));
		}
		
		long workTime = System.currentTimeMillis();
		
		collector.emit(new Values<Object>(url, type, linklist));
		
		long end = System.currentTimeMillis();
		log.info(url + " downloaded ---> working Time: " + (workTime - start) + " ms " 
				+ "emit time: " + (end - workTime) + " ms");
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
