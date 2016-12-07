package edu.upenn.cis455.crawler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Calendar;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis455.crawler.bolts.*;
import edu.upenn.cis455.crawler.info.*;
import edu.upenn.cis455.crawler.storage.*;
import edu.upenn.cis.stormlite.infrastructure.*;
import edu.upenn.cis.stormlite.tuple.Fields;


/**
 * Main class for Crawler, used to start crawl from provided start URL
 * @author cis555
 *
 */
public class XPathCrawler {
	private String startURL;
	private int maxSize;
	private DBWrapper db;
	private int maxFileNum = Integer.MAX_VALUE;
	public static URLFrontierQueue urlQueue;
	public static String dbPath;
	static Logger log = Logger.getLogger(XPathCrawler.class);
	
	public XPathCrawler(){}
	
	public XPathCrawler(String startURL, String dbStorePath, int maxSize){
		this.startURL = startURL;
		this.maxSize = maxSize;
		this.urlQueue = new URLFrontierQueue(maxSize);
		this.db = DBWrapper.getInstance(dbStorePath);
		PageDownloader.setup(db);
	}
	
	public XPathCrawler(String startURL, String dbStorePath, int maxSize, int maxFileNum){
		this(startURL, dbStorePath, maxSize);
		this.maxFileNum = maxFileNum;
	}
	
	public void close(){
		db.close();
	}
	
	public void stormCRAWL() {
		String URL_SPOUT = "URL_SPOUT";
	    String CRAWLER_BOLT = "CRAWLER_BOLT";
	    String DOWNLOAD_BOLT = "DOWNLOAD_BOLT";
	    //String MATCH_BOLT = "MATCH_BOLT";
	    String FILTER_BOLT = "FILTER_BOLT";
	    String RECORD_BOLT = "RECORD_BOLT";
	    
        Configuration config = new Configuration();
      
        DBWrapper db = DBWrapper.getInstance("dbPath");
		if(db.getFrontierQueueSize() == 0)
        	this.urlQueue.pushURL(startURL);
        
        URLSpout spout = new URLSpout();
        CrawlerBolt boltA = new CrawlerBolt();
        DownloadBolt boltB = new DownloadBolt();
//        MatchBolt boltC = new MatchBolt();
        FilterBolt boltD = new FilterBolt();
        RecordBolt boltE = new RecordBolt();
        
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(URL_SPOUT, spout, 2);
        builder.setBolt(CRAWLER_BOLT, boltA, 2).fieldsGrouping(URL_SPOUT, new Fields("URL"));
        
        builder.setBolt(DOWNLOAD_BOLT, boltB, 4).shuffleGrouping(CRAWLER_BOLT);
        //builder.setBolt(MATCH_BOLT, boltC, 4).shuffleGrouping(DOWNLOAD_BOLT);
        builder.setBolt(FILTER_BOLT, boltD, 15).shuffleGrouping(DOWNLOAD_BOLT);
        builder.setBolt(RECORD_BOLT, boltE, 150).shuffleGrouping(FILTER_BOLT);

        LocalCluster cluster = new LocalCluster();
        Topology topo = builder.createTopology();

        ObjectMapper mapper = new ObjectMapper();
		try {
			String str = mapper.writeValueAsString(topo);
			
			System.out.println("The StormLite topology is:\n" + str);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        
//        cluster.submitTopology("crawler", config, 
//        		builder.createTopology());
        
		
		
//        int targetExit = 100000;
//        int EXIT = targetExit;
//        
//        while(urlQueue.getExecutedSize() < maxFileNum) {
//        	int size = urlQueue.getSize();
//        	// keep waiting...
//            if(size == 0) {
//            	try{
//            		Thread.sleep(10000);      // if queue is empty, wait five more seconds to see whether more links are coming
//            	} catch (InterruptedException e){
//            		e.printStackTrace();
//            	}
//            	System.out.println("Empty Queue detected ---> Now Queue size: " + size);
//            	if(urlQueue.isEmpty()) EXIT--;
//            	if(EXIT == 0) break;
//            } else if(size % 100 == 0) {
//            	log.info("urlQueue size: " + size);
//            	try{
//            		Thread.sleep(3000);      // if queue is empty, wait five more seconds to see whether more links are coming
//            	} catch (InterruptedException e){
//            		e.printStackTrace();
//            	}
//            } else {
//            	long visitedsize = db.getVisitedSize();
//            	log.info("visitedURL SIZE -----> " + visitedsize);
//            	try{
//            		Thread.sleep(600000);      // if queue is empty, wait five more seconds to see whether more links are coming
//            	} catch (InterruptedException e){
//            		e.printStackTrace();
//            	}
//            	EXIT = targetExit;
//            }
//        }
        
        cluster.killTopology("crawler");
        cluster.shutdown();
        System.exit(0);
    }
	
	public static void main(String[] args) throws IOException{
		PropertyConfigurator.configure("./resources/log4j.properties");
		
		FileReader f = null;
		try {
			f = new FileReader(new File("./conf/config.properties"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		BufferedReader bf = new BufferedReader(f);
		System.setProperty("KEY", bf.readLine());
		System.setProperty("ID", bf.readLine());
		
		
		if(args.length == 0){
			System.out.println("You need to specify the arguments.");
			System.out.println("name: Tianxiang Dong");
			System.out.println("pennkey: dtianx");
			return;
		}
		else if(args.length == 3){
			String seedURL = args[0];
			String filepath = args[1];
			int maxSize = Integer.parseInt(args[2]);
			XPathCrawler.dbPath = filepath;
			XPathCrawler crawler = new XPathCrawler(seedURL, filepath, maxSize);
			crawler.stormCRAWL();
			crawler.close();
		}
		else if(args.length == 4){
			String seedURL = args[0];
			String filepath = args[1];
			int maxSize = Integer.parseInt(args[2]);
			int fileno = Integer.parseInt(args[3]);
			XPathCrawler.dbPath = filepath;
			XPathCrawler crawler = new XPathCrawler(seedURL, filepath, maxSize, fileno);
			//crawler.crawl();
			crawler.stormCRAWL();
			crawler.close();
		}
		else{
			System.out.println("The number of arguments is wrong.");
		}
	}
}
