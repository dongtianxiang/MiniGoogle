package edu.upenn.cis455.crawler;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

import edu.upenn.cis455.crawler.bolts.*;
import edu.upenn.cis455.crawler.info.Client;
import edu.upenn.cis455.crawler.storage.DBWrapper;

/**
 * The URL Frontier class, used for storing URL Queue, as well as visited URLs.
 * @author cis555
 *
 */

public class URLFrontierQueue {
	
//	private ConcurrentHashMap<String, Long> visitedURLs = new ConcurrentHashMap<String, Long>();
	private Queue<String> queue = new LinkedBlockingQueue<String>();
	private Queue<String> nextQueue = new LinkedBlockingQueue<String>();
	
	
	private int maxSize = Integer.MAX_VALUE;
	private volatile static int URLexecuted = 0;
	private static Logger log = Logger.getLogger(URLFrontierQueue.class);
	private DBWrapper db = DBWrapper.getInstance(XPathCrawler.dbPath);
	
	public URLFrontierQueue(){
		
	}
	
	public URLFrontierQueue(int maxSize){
		this.maxSize = maxSize;
		queue = new LinkedBlockingQueue<String>();
		nextQueue = new LinkedBlockingQueue<String>();
	}
	
	private synchronized void fetchFrontierQueueFromDisk(){
		if(queue.isEmpty()) {
			pushingFrontierQueueIntoDisk();
			db.pollFromFrontierQueue(1000, queue);
			log.info("Fetching Frontier Queue From disk --> Now DB Remaining: " + db.getFrontierQueueSize());
		}
	}
	
	private synchronized void pushingFrontierQueueIntoDisk(){
		db.addIntoFrontierQueue((LinkedBlockingQueue<String>)nextQueue);
		log.info("Pushing Frontier Queue Into disk --> Now DB Remaining: " + db.getFrontierQueueSize());
	}
	
	public void putIntoVisitedURL(String url, Long visitedTime) {
		db.putVisitedURL(url, visitedTime);
//		visitedURLs.put(url, visitedTime);
	}
	
	public long getVisitedURLSize(){
		return db.getVisitedSize();
//		return visitedURLs.size();
	}
	
	public boolean filter(String url){
		Client client = new Client(url);
		if(!client.isValid(maxSize)) {
			log.debug(url + ": Not Downloading");
			return false;
		}
		long currentLastModified = client.getLast_modified();
		if(db.visitedURLcontains(url)){
//		if(visitedURLs.containsKey(url)){
			
			long crawled_LastModified = db.getVisitedTime(url);	
//			long crawled_LastModified = visitedURLs.get(url);
			if(currentLastModified > crawled_LastModified){
				db.putVisitedURL(url, currentLastModified);
//				visitedURLs.put(url, currentLastModified);
				return true;
			}
			else{
				log.debug(url + ": Not Modified");
				return false;
			}
		}
		return true;
	}
	
	public boolean isEmpty(){
		if(queue.isEmpty()) {
			if(db.isEmptyFrontierQueue()){
				db.addIntoFrontierQueue(nextQueue);
			} 
			db.pollFromFrontierQueue(1000, queue);
		}
		return queue.isEmpty();
	}
	
	public int getSize(){
//		return db.getFrontierQueueSize();
		return queue.size();
	}
	
	public String popURL(){
//		return db.pollFromFrontierQueue();
		fetchFrontierQueueFromDisk();
		return queue.poll();
	}
	
	public void pushURL(String url){
//		db.addIntoFrontierQueue(url);
		nextQueue.offer(url);
		updateFrontierQueuePeriodically(5000);
	}
	
	public synchronized void updateFrontierQueuePeriodically(int num) {
		if(nextQueue.size() > num) {
			pushingFrontierQueueIntoDisk();
		}
	}
	
	public synchronized int addExecutedSize(){
		return URLexecuted++;
	}
	
	public synchronized int getExecutedSize(){
		return URLexecuted;
	}
	
//	public void setLastModifiedWhenDownloading(String url){
//		Client client = new Client(url);
//		long lastModified = client.getLast_modified();
//		db.putVisitedURL(url, lastModified);
//	}
	
	public void setOutLinks(String url, Queue<String> linksToCheck) {
		db.setOutLinks(url, linksToCheck);
	}
}
