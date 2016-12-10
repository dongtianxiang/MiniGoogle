package edu.upenn.cis455.stormlite.index;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.jsoup.*;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.jsoup.Jsoup;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.jsoup.select.Elements;
import edu.stanford.nlp.simple.*;


public class TestIndexerLocal {
	// Entry-point for indexer
	private static Hashtable<String, Integer> stops = new Hashtable<>();
	private static Logger log = Logger.getLogger(TestIndexerLocal.class);
	
	public static void main(String[] args) {
		// prepare stoplist
		PropertyConfigurator.configure("./resources/log4j.properties");
		File stop = new File("./stopwords.txt");
		Scanner sc;
		try {
			sc = new Scanner(stop);
			while (sc.hasNext()) {
				String w = sc.nextLine();
				stops.put(w, 1);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			log.error(sw);
		}
	
		File doc = new File(args[0]);
		
		if (!doc.exists()) {
			log.fatal("File not exist.");
			System.exit(-1);
		}
		parse(doc, "google.com");
	}
	
	/**
	 * Use Standford CoreNLP simple API to tokenize and lemmatize words
	 * Store HITs of English words only in a hashtable (just for now)
	 * Legal HITs format: english words, english words with numbers(eg,iphone7) 
	 * Replace hyphen("-") with space, store both words with hyphen and no hyphen, eg san fransico = san-fransico
	 * @param doc
	 */
	public static void parse(File doc, String url){
		int legalWords = 0;
		int allWords = 0;
		try {
			org.jsoup.nodes.Document d = Jsoup.parse(doc, "UTF-8", "");
			d.select(":containsOwn(\u00a0)").remove();
			Elements es = d.select("*");
			// regex filter to get only legal words
			Pattern pan = Pattern.compile("[a-zA-Z0-9.@-]+");
			Pattern pan2 = Pattern.compile("[a-zA-Z]+");
			Pattern pan3 = Pattern.compile("[0-9]+,*[0-9]*");			
			for (Element e: es) {
				String nodeName = e.nodeName(), text = e.ownText().trim();
				log.info(e.nodeName() + ": " + e.ownText());			
				if (text != null && !text.isEmpty() && text.length() != 0 ){					
					edu.stanford.nlp.simple.Document tagContent = new edu.stanford.nlp.simple.Document(text);
					List<edu.stanford.nlp.simple.Sentence> sentences = tagContent.sentences();
					for (edu.stanford.nlp.simple.Sentence s: sentences) {
						log.info("sentence:" + s);
						List<String> words = s.lemmas();
						Matcher m, m2, m3;
						for (String w: words) {
							allWords++;
							w = w.trim();	// trim
							m = pan.matcher(w);
							m2 = pan2.matcher(w);
							m3 = pan3.matcher(w);
							if (m.matches()) {
								if (m2.find()){
									if (!w.equalsIgnoreCase("-rsb-")&&!w.equalsIgnoreCase("-lsb-")
											&&!w.equalsIgnoreCase("-lrb-")&&!w.equalsIgnoreCase("-rrb-")
											&&!w.equalsIgnoreCase("-lcb-")&&!w.equalsIgnoreCase("-rcb-")){
										w = w.toLowerCase();
										String value;
										if ( !stops.containsKey(w)) {
											// all legal words must be indexed with weight
											legalWords++;
											String weight = "1";
											if (nodeName.equalsIgnoreCase("title")) {
												weight = "2";
											} 
											value = url + ":" + nodeName + ":" + weight;
										} else {
											value = url;
										}
										log.info("key: " + w + " value: " + value);
									}
								} else {
									// illegal word: extract number only - eg 2014
									// only index but no weight
									if (m3.matches()) {
										log.info("number:" + w);
										String value = url;
									}	
								}
							}
							// illegal word, extract numbers only - 36,000 -> 36000
							// only index but no weight
							else {
								if (m3.matches()){
									w = w.replaceAll(",", "");
									String value = url;
									log.info("number:" + w);
								}
							}
						}
					}	
				}				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
