package edu.upenn.cis455.searchengine;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class DataWork implements Runnable {
	
	private String word;
	private int index;
	private Hashtable<String, Set<String>> temp;
	
	public DataWork(String word, Hashtable<String, Set<String>> resultTable, int index) {
		this.word = word;
		this.index = index;
		temp = resultTable;
	}
	

	@Override
	public void run() {	
		synchronized (this) {
			if (index == 1) {
				Set<String> l = new HashSet<String>();
				l.add("doc1:0.25");
				l.add("doc2:0.3");
				l.add("doc3:0.1");
				temp.put("apple", l);					
			} else {				
				Set<String> l = new HashSet<String>();
				l.add("doc1:0.35");
				l.add("doc4:0.3");
				l.add("doc6:0.1");
				temp.put("company", l);
			}
		}
	}
}
