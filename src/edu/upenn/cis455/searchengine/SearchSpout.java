package edu.upenn.cis455.searchengine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.infrastructure.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.infrastructure.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.spouts.IRichSpout;
import edu.upenn.cis.stormlite.spouts.SpoutOutputCollector;
import edu.upenn.cis.stormlite.spouts.pagerank.RankDataSpout;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.database.DBInstance;
import edu.upenn.cis455.database.DBManager;
import edu.upenn.cis455.database.Node;

public class SearchSpout implements IRichSpout {
	
	public static Logger log = Logger.getLogger(RankDataSpout.class);
	public String executorId = UUID.randomUUID().toString();
	public SpoutOutputCollector collector;
	public Fields schema = new Fields("key", "value");	
	@SuppressWarnings("rawtypes")
	public Map config;
	public boolean eofSent = false;	
	private File docFile;
	private Scanner scanner;

	@Override
	public String getExecutorId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(schema);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void open(Map<String, String> conf, TopologyContext topo, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	    config = conf;
	    config.put("status", "IDLE");
	        
        String inputFileDir = (String)conf.get("inputDir");
		docFile = new File(inputFileDir, "query.txt");
		if (!docFile.exists()) {
			try {
				docFile.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
			
		try  {
			scanner = new Scanner(docFile);
			if (!scanner.hasNext()) {
				throw new IllegalStateException();
			}
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		if (scanner != null) {
			if (eofSent) {
				scanner.close();
				scanner = null;
				System.out.println("File reader instance has been closed");
			}
		}
	}

	@Override
	public void nextTuple() {		
		if (scanner != null && !eofSent) {
			try {
				
				String pair = null;
				if (scanner.hasNext()) {
					pair = scanner.nextLine();
				}				
				if (pair != null && pair.length() > 0) {
					collector.emit(new Values<Object>("", pair));
				}
				else if (!eofSent) {
					collector.emitEndOfStream();
					eofSent = true;
				}				
			} 
			catch (Exception ioe) {
				ioe.printStackTrace();
			}
		}
		Thread.yield();
	}

	@Override
	public void setRouter(StreamRouter router) {
		collector.setRouter(router);
	}

}
