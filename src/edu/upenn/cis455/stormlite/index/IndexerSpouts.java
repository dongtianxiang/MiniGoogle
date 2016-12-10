package edu.upenn.cis455.stormlite.index;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.infrastructure.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.infrastructure.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.spouts.IRichSpout;
import edu.upenn.cis.stormlite.spouts.SpoutOutputCollector;
import edu.upenn.cis.stormlite.spouts.bdb.LinksFileSpout;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.XPathCrawler;
import edu.upenn.cis455.crawler.storage.DBWrapper;

public class IndexerSpouts implements IRichSpout{
	public static Logger log = Logger.getLogger(LinksFileSpout.class);
	public String executorId = UUID.randomUUID().toString();
	public SpoutOutputCollector collector;
	public Fields schema = new Fields("url");	
	@SuppressWarnings("rawtypes")
	public Map config;
	public AtomicBoolean eofSent;	
	public String serverIndex;
	private DBWrapper db;
	private String dbPath = "./dtianx";
	
	public IndexerSpouts() {
		
	}

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(schema);
		
	}

	@Override
	public void open(Map<String, String> config, TopologyContext topo, SpoutOutputCollector collector) {
		this.config = config;
		this.collector = collector;
		config.put("status", "IDLE");
		if (config.containsKey("workerIndex")) {
    		serverIndex = (String)config.get("workerIndex");
    		db  = DBWrapper.getInstance(dbPath +serverIndex);
    	}
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		db.close();
	}

	@Override
	public void nextTuple() {
		if (db != null && !eofSent.get()) {
//			Queue<String> memoryQueue = new LinkedList<>();
//			db.pollFromFrontierQueue(1000, memoryQueue);
//			while(!memoryQueue.isEmpty()) {
//				while(!memoryQueue.isEmpty()) {
//					String url = memoryQueue.poll();
//					collector.emit(new Values<Object>(url));
//				}
//				db.pollFromFrontierQueue(1000, memoryQueue);
//			}
			List<String> urls = db.outLinksList();
			for(String url: urls) {
				collector.emit(new Values<Object>(url));
			}
			log.info("Server#"+serverIndex+" Finished reading urls from db" + " and emitting EOS");
			collector.emitEndOfStream();
			eofSent.set(true);
			
		} else {
			log.fatal("Server#"+serverIndex+": db is null or eos received in spouts");
		}	
		Thread.yield();
	}

	@Override
	public void setRouter(StreamRouter router) {
		collector.setRouter(router);
	}

}
