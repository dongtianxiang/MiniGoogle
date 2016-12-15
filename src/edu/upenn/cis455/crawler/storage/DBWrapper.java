package edu.upenn.cis455.crawler.storage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;
import edu.upenn.cis455.crawler.*;
import edu.upenn.cis455.crawler.info.URLInfo;

/**
 * Basic class to connect Berkeley DB, including add and get User, Page, etc. from Database.
 * @author cis555
 *
 */
public class DBWrapper {
	
	private static String envDirectory = null;
	
	private static Environment myEnv;
	private static EntityStore store;
	
	private static DBWrapper DBinstance = null;
	
	PrimaryIndex<String, OutLinks> outLinksIndex;
	PrimaryIndex<String, VisitedURL> visitedURLIndex;
	PrimaryIndex<String, FrontierQueue> frontierQueueIndex;
	PrimaryIndex<String, RobotMap> RobotMapIndex;
	static Logger log = Logger.getLogger(DBWrapper.class);
	
	/* TODO: write object store wrapper for BerkeleyDB */
	private DBWrapper(String envDirectory){
		//Initialize myEnv
		this.envDirectory = envDirectory;
		try{
			EnvironmentConfig envConfig = new EnvironmentConfig();
			//Create new myEnv if it does not exist
			envConfig.setLockTimeout(500, TimeUnit.MILLISECONDS);
			envConfig.setAllowCreate(true);
			//Allow transactions in new myEnv
			envConfig.setTransactional(true);
			//Create new myEnv
			File dir = new File(envDirectory);
			if(!dir.exists())
			{
				dir.mkdir();
				dir.setReadable(true);
				dir.setWritable(true);
			}
			myEnv = new Environment(dir,envConfig);
			
			//Create new entity store object
			StoreConfig storeConfig = new StoreConfig();
			storeConfig.setAllowCreate(true);
			storeConfig.setTransactional(true);
			store = new EntityStore(myEnv,"DBEntityStore",storeConfig);
			
			outLinksIndex = store.getPrimaryIndex(String.class, OutLinks.class);
			visitedURLIndex = store.getPrimaryIndex(String.class, VisitedURL.class);
			frontierQueueIndex = store.getPrimaryIndex(String.class, FrontierQueue.class);
			RobotMapIndex = store.getPrimaryIndex(String.class, RobotMap.class);
			
		}
		catch(DatabaseException e)
		{
			e.printStackTrace();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public synchronized static DBWrapper getInstance(String envDirectory) {
		if(DBinstance == null) {
			close();
			DBinstance = new DBWrapper(envDirectory);
		}
		return DBinstance;
	}
	
	public void sync() {
		if(store != null) store.sync();
		if(myEnv != null) myEnv.sync();
	}
	
	public Environment getEnvironment() {
		return myEnv;
	}
	
	public EntityStore getStoreUser() {
		return store;
	}
	
	public EntityStore getStoreCrawler() {
		return store;
	}
	
	//Close method
	public synchronized static void close() {
		
		//Close store first as recommended
		if(store!=null) {
			try{
				store.close();
			}
			catch(DatabaseException e)
			{
				e.printStackTrace();
			}
		}
		
		
		if(myEnv!=null) {
			try{
				myEnv.close();
			}
			catch(DatabaseException e)
			{
				e.printStackTrace();
			}
		}
		DBinstance = null;
	}
	

	public Page getWebpage(String url) {
		PrimaryIndex<String, Page> WebpageIndex = store.getPrimaryIndex(String.class, Page.class);
		return WebpageIndex.get(url);
	}
	
	/* Related Method for OutLinks */
	
	public synchronized OutLinks getOutLinks(String url) {
		synchronized(outLinksIndex) {
			return outLinksIndex.get(url);
		}
	}
	
	public synchronized void putOutLinks(OutLinks outlinks) {
		synchronized(outLinksIndex) {
			outLinksIndex.put(outlinks);
		}
		sync();
	}
	
	public List<String> getOutLinksList(String url){
		OutLinks outlinks = getOutLinks(url);
		return outlinks.getLinks();
	}
	
	public void addOutLinks(String url, String link) {
		OutLinks outlinks = getOutLinks(url);
		if(outlinks == null) {
			outlinks = new OutLinks(url);
		}
		outlinks.addLinks(link);
		putOutLinks(outlinks);
	}
	
	public void setOutLinks(String url, Queue<String> links) {
		OutLinks outlinks = getOutLinks(url);
		if(outlinks == null) {
			outlinks = new OutLinks(url);
			for(int i = 0; i < links.size(); i++) {
				String link = links.poll();
				outlinks.getLinks().add(link);
				links.offer(link);
			}
			putOutLinks(outlinks);
		}
	}
	
	public long getOutLinksSize(){
		synchronized(outLinksIndex) {
			return outLinksIndex.count();
		}
	}
	
	public List<String> outLinksList() {
		synchronized(outLinksIndex) {
			List<String> res = new ArrayList<String>();
			EntityCursor<OutLinks> entities = this.outLinksIndex.entities();
			for(OutLinks url : entities) {
				res.add(url.getUrl());
			}
			entities.close();
			return res;
		}
	}
	
	/* Related Method for VisitedURLs */
	
	public VisitedURL getVisitedURL(String url) {
		synchronized(visitedURLIndex) {
			return visitedURLIndex.get(url);
		}
	}
	
	public synchronized void putVisitedURL(VisitedURL visitedURL) {
		synchronized(visitedURLIndex) {
			visitedURLIndex.put(visitedURL);
		}
		sync();
	}
	
	public Long getVisitedTime(String url) {
		VisitedURL v = getVisitedURL(url);
		if(v == null) return Calendar.getInstance().getTimeInMillis();
		return v.getLastVisited();
	}

	public void putVisitedURL(String url, Long lastVisited) {
		VisitedURL v = getVisitedURL(url);
		if(v == null) {
			v = new VisitedURL(url, lastVisited);
		} else {
			v.setLastVisited(lastVisited);
		}
		putVisitedURL(v);
	}
	
	public long getVisitedSize(){
		synchronized(visitedURLIndex) {
			return visitedURLIndex.count();
		}
	}
	
	public boolean visitedURLcontains(String url) {
		synchronized(visitedURLIndex) {
			return visitedURLIndex.contains(url);
		}
	}
	
	public List<String> visitedURLList() {
		synchronized(visitedURLIndex) {
			List<String> res = new ArrayList<String>();
			EntityCursor<VisitedURL> entities = this.visitedURLIndex.entities();
			for(VisitedURL url : entities) {
				res.add(url.getUrl());
			}
			entities.close();
			return res;
		}
	}
	
	/* Related Method for FrontierQueue */
	public FrontierQueue getFrontierQueue() {
		synchronized(frontierQueueIndex) {
			FrontierQueue queue = frontierQueueIndex.get("FrontierQueue");
			if(queue == null) {
				queue = new FrontierQueue();
				putFrontierQueue(queue);
			}
			return queue;
		}
	}
	
	public void putFrontierQueue(FrontierQueue queue){
		synchronized(frontierQueueIndex) {
			frontierQueueIndex.put(queue);
		}
		sync();
	}
	
	public synchronized void pollFromFrontierQueue(int num, Queue<String> memoryQueue){
		FrontierQueue queue = getFrontierQueue();
		int count = 0;
		while(!queue.isEmpty() && count < num) {
			String url = queue.pollQueue();
			memoryQueue.offer(url);
			count++;
		}
		putFrontierQueue(queue);
	}
	
	public synchronized void addIntoFrontierQueue(Queue<String> nextQueue){
		FrontierQueue queue = getFrontierQueue();
		while(!nextQueue.isEmpty()) {
			queue.addQueue(nextQueue.poll());
		}
		putFrontierQueue(queue);
	}
	
	public int getFrontierQueueSize() {
		FrontierQueue queue = getFrontierQueue();
		int size = queue.getSize();
		return size;
	}
	
	public boolean isEmptyFrontierQueue() {
		return getFrontierQueueSize() == 0;
	}
	
	/* Related Method for RobotMap */
	public RobotMap getRobotMap(String hostname) {
		synchronized(RobotMapIndex) {
			return RobotMapIndex.get(hostname);
		}

	}
	
	public void putRobotMap(RobotMap RobotMap) {
		synchronized(RobotMapIndex) {
			RobotMapIndex.put(RobotMap);
		}
	}
	
	public void putRobotMap(String hostname, String url) {
		RobotMap v = getRobotMap(hostname);
		if(v == null) {
			v = new RobotMap(hostname, url);
			putRobotMap(v);
		} 
	}

	public int getRobotCrawlDelay(String hostname) {
		return getRobotMap(hostname).getCrawlDelay();
	}
	
	public boolean getRobotIsURLValid(String hostname, String url) {
		RobotMap robot = getRobotMap(hostname);
		boolean res = robot.isURLValid(url);     // update max visited on Host
		//if(res) putRobotMap(robot);
		return res;
	}
	
	public long getRobotLastVisited(String hostname){
		return getRobotMap(hostname).getLastVisited();
	}
	
	public long getRobotVisitedSize(String hostname){
		return getRobotMap(hostname).getVisitedSize();
	}
	
	public void setRobotLastVisited(String hostname){
		RobotMap v = getRobotMap(hostname);
		v.setLastVisited();
		putRobotMap(v);
	}
	
	public long getRobotMapSize(){
		synchronized(RobotMapIndex) {
			return RobotMapIndex.count();
		}
	}
	
	public boolean RobotMapContains(String hostName) {
		if(hostName == null) return false;
		synchronized(RobotMapIndex) {
			return RobotMapIndex.contains(hostName);
		}
	}
	
	
	
	
	public static void main(String[] args) throws IOException{
		DBWrapper db = DBWrapper.getInstance("./dtianx0");
//		String URL = "http://www.alumni.upenn.edu/s/1587/gid2/16/start.aspx?sid=1587&gid=2&pgid=731";
//		String keyName = DigestUtils.sha1Hex(URL); 
//		System.out.println(keyName);
		
		System.out.println(db.getFrontierQueueSize());
		System.out.println(db.getOutLinksSize());
		System.out.println("getting data from dtianx0");
		List<String> res = db.outLinksList();
		Set<String> set0 = new HashSet<>();
		System.out.println("adding data into set");
		for(String url : res) {
			set0.add(url);
//			System.out.println(url);
//			System.out.println("  db1 contains: " + db1.visitedURLcontains(url));
		}
		System.out.println();
		System.out.println("************************");
		System.out.println();
		db.close();
		
		DBWrapper db1 = DBWrapper.getInstance("./dtianx1");
		System.out.println("getting data from dtianx1");
		
		List<String> res1 = db1.outLinksList();
		System.out.println("checking start: ");
		for(String url : res1) {
//			System.out.println(url);
			if(set0.contains(url)) System.err.println(url + " contained in both");
		}
		System.out.println("checking ended! ");

	}
}
