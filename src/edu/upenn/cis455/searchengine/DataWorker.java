package edu.upenn.cis455.searchengine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.amazonaws.auth.AWSCredentials;

import edu.upenn.cis455.crawler.PageDownloader;

public class DataWorker implements Runnable {
	
	static Logger log = Logger.getLogger(DataWorker.class);	
	
	private String url;
	private AWSCredentials credentials;
	private List<Hashtable<String, String>> resultList;
	final static int DLIMIT = 300;
	final static int TLIMIT = 80;
	private String title = "", description = "", query = "";
	private int i;
	private String[] queryList ;
	
	public DataWorker(int i, String url, AWSCredentials credentials, List<Hashtable<String, String>> resultList, String query) {
		this.url = url;
		this.credentials = credentials;
		this.resultList = resultList;
		this.i = i;
		this.query = query;
		queryList = query.split(" ");
	}

	@Override
	public void run() {
		StringBuilder sb = new StringBuilder();
//		url = url.replace("/", ".");
//		File in = new File("./testpage/" + url + ".html");
		try {
			InputStream in = PageDownloader.downloadfileS3(credentials, url);
			Scanner sc;
			sc = new Scanner(in);
			while (sc.hasNext()) {
				sb.append(sc.nextLine() + "\n");
			}
		} catch (Exception e) {
			log.info(e);
		}
		getDescription(sb.toString());		
	}
	
	public void getDescription(String input){
		Document d = Jsoup.parse(input, "utf-8");
		Elements titles = d.select("title");
		if (titles != null) {
			for (Element e: titles) {
				title += " " + e.ownText();
			}
		}
		if (title == null || title.length() == 0) {
			// no title tags are found
			title = url.replace("http://", "");
			title = url.replace("https://", "");
		}		
		if (title.length() > TLIMIT) {
			title = title.substring(0, TLIMIT);
			title += "...";
		} 
		// predict description
		description = "";
		int dSize = 0;
		// find meta data
		Elements em = d.select("meta[name~=\"*description*\"]");
		// no metadata with descripion are found
		if (em != null) {
			for (Element e: em) {
				if (dSize > DLIMIT) {
					description = description.substring(0, DLIMIT);
					description += "...";
					dSize += 3;
					break;
				}
				String content = e.attr("content");
				description = description + " " + content;
				dSize += content.length();
				if (e.hasText()) {
					String ownText = e.ownText();
					description += " " + ownText;
					dSize += ownText.length();
				}
			}
		} 		
		if (dSize < DLIMIT){
			Elements em2 = d.select("meta[property~=\"*description*\"]");
			if (em2 != null) {
				for (Element e: em2) {
					if (dSize > DLIMIT) {
						description = description.substring(0, DLIMIT);
						description += "...";
						dSize += 3;
						break;
					}
					String content = e.attr("content");
					description = description + " " + content;
					dSize += content.length();
					if (e.hasText()) {
						String ownText = e.ownText();
						description += ownText;
						dSize += ownText.length();
					}
				}
			}
		}
		if (dSize < DLIMIT) {
			Elements em3 = d.select("*");
			for (Element e: em3) {
				if (dSize > DLIMIT) {
					description = description.substring(0, DLIMIT);
					description += "...";
					dSize += 3;
					break;
				}
				String ownText = e.ownText();
//				for (String s: queryList) {
//					if (ownText.toLowerCase().contains(s.toLowerCase())) {
//						ownText = ownText.replace(s, "<em style=\"font-family: \"Lota\"\">" + s + "</em>");
//					}
//				}
				description += " " + ownText;
				dSize += ownText.length();
			}
		}
		putData();
	}
		
	public synchronized void putData() {
		Hashtable<String, String> map = new Hashtable<>(5);
		map.put("title", title);
		map.put("desc", description);
		map.put("url", url);
		resultList.set(i, map);
	}
	
}
