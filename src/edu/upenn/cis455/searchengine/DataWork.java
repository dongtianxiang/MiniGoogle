package edu.upenn.cis455.searchengine;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Scanner;

import org.apache.log4j.Logger;

public class DataWork implements Runnable {
	
	static Logger log = Logger.getLogger(DataWork.class);	
	
	private String word;
	private int index;
	private HashMap<String, HashMap<String, Double>> temp;
	private File f;
	private HashMap<String, Integer> lexicon = new HashMap<>();
	
	public DataWork(String word, HashMap<String, HashMap<String, Double>> resultTable, int index) {
		this.word = word;
		this.index = index;
		temp = resultTable;
		f = new File("./database" + this.index + "/db.txt");
		if (this.index == 0) {
			lexicon.put("company", 1);
		} else {
			lexicon.put("apple", 1);
		}
	}
	
	@Override
	public void run() {
		synchronized (this) {
			if (lexicon.containsKey(word)) {
				try {
					Scanner sc = new Scanner(f);
					while (sc.hasNextLine()) {
						String line = sc.nextLine();
						String[] list = line.split(":");
						String word = list[0];
						String doc = list[1];
						double weight = Double.parseDouble(list[2]);
						HashMap<String, Double> map;
						if (temp.containsKey(word)) {
							map = temp.get(word);
						} else {
							map = new HashMap<>();
						}
						map.put(doc, weight);
						temp.put(word, map);
					}
					sc.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		log.info("temp with " + this.index + " " + temp);
	}
}
