package edu.upenn.cis455.crawler;

import java.util.Calendar;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;

import edu.upenn.cis455.crawler.info.URLInfo;
import edu.upenn.cis455.crawler.storage.DBWrapper;

public class RobotCache {
	//public static ConcurrentHashMap<String, Robot> robots = new ConcurrentHashMap<>();
	private static DBWrapper db = DBWrapper.getInstance(XPathCrawler.dbPath);
	
	public static void addRobot(String url) {
		URLInfo urlinfo = new URLInfo(url);
		String hostName = urlinfo.getHostName();
		String protocol = urlinfo.getProtocol();
		if(hostName == null) return;
		
		if(!db.RobotMapContains(hostName)){
			db.putRobotMap(hostName, url);
		}
	}
	
	public static boolean isValid(String url) {
		try{
			addRobot(url);
		} catch (Exception e) {    // in case the HTTPConnection for robots.txt fails, leading to NullPointerException
			return false;
		}
		URLInfo urlinfo = new URLInfo(url);
		String hostName = urlinfo.getHostName();
		if(hostName == null) return false;
		
		return db.getRobotIsURLValid(hostName, url);
	}
	
	public static boolean checkDelay(String url){
		URLInfo urlinfo = new URLInfo(url);
		String hostName = urlinfo.getHostName();
		if(hostName == null) return true;
		
		if(db.RobotMapContains(hostName)) {    // not having hostName means even haven't crawled before 
			if(db.getRobotCrawlDelay(hostName) == 0) return true;
			Calendar cal = Calendar.getInstance();
			long lastVisited = db.getRobotLastVisited(hostName);
			long currentVisiting = cal.getTime().getTime();
			if(currentVisiting - lastVisited >= db.getRobotCrawlDelay(hostName) * 1000 ) {  // crawl delay in seconds actually
				db.setRobotLastVisited(hostName);
				return true;
			} else {
				return false;
			}
		} else {
			addRobot(url);
			return true;
		}
	}
	
	public static void setCurrentTime(String url) {
		URLInfo urlinfo = new URLInfo(url);
		String hostname = urlinfo.getHostName();
		if(hostname == null) return;
		
		if(!db.RobotMapContains(hostname)) {
			addRobot(url);
		}
		db.setRobotLastVisited(hostname);
	}
	
	public static long getLastVisited(String url) {
		URLInfo urlinfo = new URLInfo(url);
		String hostname = urlinfo.getHostName();
		return db.getRobotLastVisited(hostname);
	}
}
