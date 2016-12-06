package edu.upenn.cis455.stormlite.index;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import edu.stanford.nlp.*;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.simple.*;
import edu.stanford.nlp.util.CoreMap;
import edu.upenn.cis455.database.Hit;
import edu.upenn.cis455.database.Position;

public class TestIndexerLocal {
	// Entrypoint for indexer
	private static Hashtable<String, Hit> tables = new Hashtable<>();
	
	public static void main(String[] args) {		
		File doc = new File(args[0]);
		if (!doc.exists()) {
			System.out.println("File not exist.");
			System.exit(-1);
		}
		parse(doc);
//		parseUseCore(doc);
	}
	
	/**
	 * Use Standford CoreNLP simple API to tokenize and lemmatize words
	 * Store HITs of english words only in a hashtable (just for now)
	 * Legal HITs format: english words, english words with numbers(eg,iphone7) 
	 * Replace hyphen("-") with space, store both words with hyphen and no hyphen, eg san fransico = san-fransico
	 * @param doc
	 */
	public static void parse(File doc){
		try {
			org.jsoup.nodes.Document d = Jsoup.parse(doc, "UTF-8", "");
			d.select(":containsOwn(\u00a0)").remove();
			Elements es = d.select("*");
			// regex filter to get only legal words
			Pattern pan = Pattern.compile("[a-zA-Z0-9.@-]+");
			Pattern pan2 = Pattern.compile("[a-zA-Z]+");
						
			for (Element e: es) {
				String nodeName = e.nodeName(), text = e.ownText().trim();
//				System.out.println(e.nodeName() + ": " + e.ownText());			
				if (text != null && !text.isEmpty() && text.length() != 0 ){					
					Document tagContent = new Document(text);
					List<Sentence> sentences = tagContent.sentences();
					for (Sentence s: sentences) {
//						System.out.println("sentence:" + s);
						List<String> words = s.lemmas();
						int i = 1;
						Matcher m, m2;
						for (String w: words) {
							w = w.trim();	// trim
//							System.out.println("before:" + w);
							m = pan.matcher(w);
							m2 = pan2.matcher(w);
							if (m.matches()) {
								if (m2.find()){
									if (!w.equalsIgnoreCase("-rsb-")&&!w.equalsIgnoreCase("-lsb-")
											&&!w.equalsIgnoreCase("-lrb-")&&!w.equalsIgnoreCase("-rrb-")
											&&!w.equalsIgnoreCase("-lcb-")&&!w.equalsIgnoreCase("-rcb-")){
										w = w.toLowerCase();
										Hit h;
										if (tables.containsKey(w)){
											h = tables.get(w);
											h.increaseFrequency();
										} else {
											h = new Hit(w);
										}
										h.setDocID(1);
										if (nodeName.equalsIgnoreCase("title")) {
											h.setTitle(true);
											h.addPosition(new Position(i, 3));
										} else {
											h.addPosition(new Position(i, 1));
										}
										// not consider meta
										tables.put(w, h);
										System.out.println("hit:" + h.getText());
										
									}
								}
							}
							i++;
						}
					}	
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
//	public static void parseUseCore(File doc) {
//		// configure nlp
//		Properties myPro = new Properties();
//		myPro.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse");
//		StanfordCoreNLP pipeline = new StanfordCoreNLP(myPro);
//		org.jsoup.nodes.Document d;
//		try {
//			d = Jsoup.parse(doc, "UTF-8", "");
//			d.select(":containsOwn(\u00a0)").remove();
//			Elements es = d.select("*");
//			for (Element e: es) {
//				String nodeName = e.nodeName(), text = e.ownText().trim();		
//				if (text != null && !text.isEmpty() && text.length() != 0 ){					
//					Sentence sen = new Sentence(text);					
//					Annotation ann = new Annotation(text);
//					pipeline.annotate(ann);
//					List<CoreMap> sentences = ann.get(SentencesAnnotation.class);
//					
//					for(CoreMap sentence: sentences) {
//						System.out.println(sentence.toString());
						  // traversing the words in the current sentence
						  // a CoreLabel is a CoreMap with additional token-specific methods
//						  for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
//						    // this is the text of the token
//						    String word = token.get(TextAnnotation.class);
//						    System.out.println("word:" + word);
//						    // this is the NER label of the token
//						    String ne = token.get(NamedEntityTagAnnotation.class);
//						    System.out.println("ne:" + ne);
//						  }
//					}	
//				}
//			}
//			
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}		
//	}
		
}
