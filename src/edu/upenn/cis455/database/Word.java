package edu.upenn.cis455.database;

import com.sleepycat.persist.model.PrimaryKey;
import java.util.*;
public class Word {
	@PrimaryKey
	String text;
	
	public List<String> urls = new ArrayList<>();
	
	public void addUrls(String url) {
		if(urls == null) {
			urls = new ArrayList<>();
		}
		urls.add(url);
	}
	
	
}
