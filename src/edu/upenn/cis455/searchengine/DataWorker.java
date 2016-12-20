package edu.upenn.cis455.searchengine;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.amazonaws.auth.AWSCredentials;

public class DataWorker implements Runnable {
	
	static Logger log = Logger.getLogger(DataWorker.class);	
	
	private String url;
//	private AWSCredentials credentials;
	private List<Hashtable<String, String>> resultList;
	final static int DLIMIT = 100;
	final static int TLIMIT = 50;
	private String title, description;
	
	public DataWorker(String url, List<Hashtable<String, String>> resultList) {
		this.url = url;
//		this.credentials = credentials;
		this.resultList = resultList;
	}

	@Override
	public void run() {
		StringBuilder sb = new StringBuilder();
		url = url.replace("/", ".");
		File in = new File("./testpage/" + url + ".html");
//		InputStream in = PageDownloader.downloadfileS3(credentials, url);
		Scanner sc;
		try {
			sc = new Scanner(in);
			while (sc.hasNext()) {
				sb.append(sc.nextLine() + "\n");
			}
			sc.close();
			getDescription(sb.toString());
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	public void getDescription(String input){
		Document d = Jsoup.parse(input, "utf-8");
		title = d.select("title").first().text();
		if (title.length() > TLIMIT) {
			title = title.substring(0, 50);
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
					break;
				}
				String content = e.attr("content");
				description += content;
				dSize += content.length();
				if (e.hasText()) {
					String ownText = e.text();
					description += ownText;
					dSize += ownText.length();
				}
			}
			putData();
			return;
		} 
		
		if (em == null){
			Elements em2 = d.select("meta[property~=\"*description*\"]");
			if (em2 != null) {
				for (Element e: em2) {
					if (dSize > DLIMIT) {
						break;
					}
					String content = e.attr("content");
					description += content;
					dSize += content.length();
					if (e.hasText()) {
						String ownText = e.text();
						description += ownText;
						dSize += ownText.length();
					}
				}
				putData();
				return;
			} else {
				// select the first <p>
				String em3 = d.select("p").first().toString();
				if (em3.length() > DLIMIT) {
					em3 = em3.substring(0, DLIMIT);
				}
				description += em3;
				putData();
			}
		}
	}
		
	public synchronized void putData() {
		Hashtable<String, String> map = new Hashtable<>(5);
		map.put("title", title);
		map.put("desc", description);
		map.put("url", url);
		resultList.add(map);
	}
	
}
