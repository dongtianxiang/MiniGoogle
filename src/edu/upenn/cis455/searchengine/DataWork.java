package edu.upenn.cis455.searchengine;

import java.util.Hashtable;
import java.util.Map;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Scanner;

import org.apache.log4j.Logger;

import edu.upenn.cis455.SearchWorkerServer.SearchDBWrapper;
import edu.upenn.cis455.SearchWorkerServer.SearchWorkerServer;
import edu.upenn.cis455.SearchWorkerServer.Word;

public class DataWork{
	
	static Logger log = Logger.getLogger(DataWork.class);	
	
	private String word;
	private int index;
	private Hashtable<String, Hashtable<String, Double>> temp;
	private Hashtable<String, Integer> lexicon = new Hashtable<>();
	private SearchDBWrapper db = SearchDBWrapper.getInstance(SearchWorkerServer.tempDir);
	
	public DataWork(String word, Hashtable<String, Hashtable<String, Double>> resultTable, int index) {
		this.word = word;
		this.index = index;
		this.temp = resultTable;
	}
	
	/**
	 * Retriving data from database in background thread
	 */	
	public void generateData() {
		try {
			if(!db.containsWord(word)) {
				log.info("Worker " + this.index + " doesn't contain " + word);
				return;
			}
			Hashtable<String, Double> map = new Hashtable<>();   // URL and its weight_pg, if URL is title, add ":T" to its end
			
			long time0 = System.currentTimeMillis();
			Word w = db.getWord(word);
			Map<String, Double> weights = w.getWeightWithPG();
			//TODO: ADD data into Map, converting into hashing and add ":T"
			Map<String, Boolean> istitle = w.getTitle();
			
			long time1 = System.currentTimeMillis();
			for(String url : weights.keySet()) {
				Double weight_with_pg = weights.get(url);
				String realURL = db.convertHashing(url); 
				String urlWithTitle = realURL;
				if(istitle.containsKey(url)) urlWithTitle = urlWithTitle + " T";
				map.put(urlWithTitle, weight_with_pg);
			}
			long time2 = System.currentTimeMillis();
			log.info("step0: " + (time1 - time0) + "ms step1: " + (time2-time1) + "ms");
			
			temp.put(word, map);
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e.getMessage());
		}
	}
}
