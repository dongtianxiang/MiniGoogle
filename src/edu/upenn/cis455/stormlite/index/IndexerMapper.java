package edu.upenn.cis455.stormlite.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

import edu.upenn.cis.stormlite.bolts.IRichBolt;
import edu.upenn.cis.stormlite.bolts.OutputCollector;
import edu.upenn.cis.stormlite.bolts.bdb.BuilderMapBolt;
import edu.upenn.cis.stormlite.infrastructure.Job;
import edu.upenn.cis.stormlite.infrastructure.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.infrastructure.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.PageDownloader;
import edu.upenn.cis455.crawler.storage.DBWrapper;

public class IndexerMapper implements IRichBolt {
	public static Logger log = Logger.getLogger(IndexerMapper.class);
	public Map<String, String> config;
	public String executorId = UUID.randomUUID().toString();
	public Fields schema = new Fields("word", "url");
	public OutputCollector collector;
	public Integer eosNeeded = 0;
	public String serverIndex = null;
	private DBWrapper db;
	private static Set<String> stops = new HashSet<>();
	private Map<String, Integer> weights = new HashMap<>();
	static {
		File stop = new File("./stopwords.txt");
		Scanner sc;
		try {
			sc = new Scanner(stop);
			while (sc.hasNext()) {
				String w = sc.nextLine();
				stops.add(w);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private TopologyContext context;
	AWSCredentials credentials;

	public IndexerMapper() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(schema);

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple input) {
		String url = input.getStringByField("url");
		InputStream in = PageDownloader.downloadfileS3(credentials, url);
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String line;
		try {
			while((line = reader.readLine())!=null) {
			System.err.println(line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		this.config = stormConf;
		this.context = context;
		this.collector = collector;
		
		credentials = new BasicAWSCredentials(System.getProperty("KEY"), System.getProperty("ID"));
		int numMappers = Integer.parseInt(stormConf.get("mapExecutors"));
		int numSpouts = Integer.parseInt(stormConf.get("spoutExecutors"));
		int numWorkers = Integer.parseInt(stormConf.get("workers"));
		eosNeeded = (numWorkers - 1) * numSpouts * numMappers + numSpouts;
		log.info("Num EOS required for MapBolt: " + eosNeeded);

	}

	@Override
	public void setRouter(StreamRouter router) {
		collector.setRouter(router);

	}

	@Override
	public Fields getSchema() {
		return schema;
	}
	
	/**
	 * Use Standford CoreNLP simple API to tokenize and lemmatize words
	 * Store HITs of English words only in a hashtable (just for now)
	 * Legal HITs format: english words, english words with numbers(eg,iphone7) 
	 * Replace hyphen("-") with space, store both words with hyphen and no hyphen, eg san fransico = san-fransico
	 * @param doc
	 * @throws IOException 
	 */
	public void parse(InputStream in, List<String> keyWords, String url) throws IOException{
		int legalWords = 0;
		int allWords = 0;
		try {
			org.jsoup.nodes.Document d = Jsoup.parse(in, "UTF-8", "");
			d.select(":containsOwn(\u00a0)").remove();
			Elements es = d.select("*");
			// regex filter to get only legal words
			Pattern pan = Pattern.compile("[a-zA-Z0-9.@-]+");
			Pattern pan2 = Pattern.compile("[a-zA-Z]+");
			Pattern pan3 = Pattern.compile("[0-9]+,*[0-9]*");			
			for (Element e: es) {
				String nodeName = e.nodeName(), text = e.ownText().trim();
				System.out.println(e.nodeName() + ": " + e.ownText());			
				if (text != null && !text.isEmpty() && text.length() != 0 ){					
					edu.stanford.nlp.simple.Document tagContent = new edu.stanford.nlp.simple.Document(text);
					List<edu.stanford.nlp.simple.Sentence> sentences = tagContent.sentences();
					for (edu.stanford.nlp.simple.Sentence s: sentences) {
						//System.out.println("sentence:" + s);
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
										if ( !stops.contains(w)) {
											// all legal words must be indexed with weight
											legalWords++;
											int weight = 1;
											if (nodeName.equalsIgnoreCase("title")) {
												weight = 2;
											}
											if(!weights.containsKey(w)) {
												collector.emit(new Values<Object>(w, url));
												weights.put(w, weight);
											} else {
												weights.put(w, weights.get(w)+weight);
											}
//											value = url + ":" + nodeName + ":" + weight;
										} 
//											else {
//											value = url;
//										}
//										System.out.println("key: " + w + " value: " + value);
									}
								} else {
									// illegal word: extract number only - eg 2014
									// only index but no weight
									if (m3.matches()) {
//										System.out.println("number:" + w);
//										String value = url;
									}	
								}
							}
							// illegal word, extract numbers only - 36,000 -> 36000
							// only index but no weight
							else {
								if (m3.matches()){
//									w = w.replaceAll(",", "");
//									String value = url;
//									System.out.println("number:" + w);
								}
							}
						}
					}	
				}				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			in.close();
		}
	}


}
