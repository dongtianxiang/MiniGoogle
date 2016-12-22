package edu.upenn.cis455.SearchWorkerServer;

import static spark.Spark.setPort;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.HashSet;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.infrastructure.Configuration;
import edu.upenn.cis.stormlite.infrastructure.DistributedCluster;
import edu.upenn.cis.stormlite.infrastructure.TopologyContext;
import edu.upenn.cis.stormlite.infrastructure.WorkerHelper;
import edu.upenn.cis455.searchengine.DataWork;
import spark.Request;
import spark.Response;
import spark.Route;
import spark.Spark;

public class SearchWorkerServer {
	
	static Logger log = Logger.getLogger(SearchWorkerServer.class);	
	public static DistributedCluster cluster;
	private static Pattern pan = Pattern.compile("^[a-zA-Z0-9]+[.@&-]*[a-zA-Z0-9]+");
    List<TopologyContext> contexts = new ArrayList<>();
	static List<String> topologies = new ArrayList<>();
	private static Set<String> bigramDict = new HashSet<>();
	private static Set<String> trigramDict = new HashSet<>();
	private static Set<String> stops = new HashSet<>();
	static {
		List<String> list = Arrays.asList("need", "also", "play", "feel", "felt", "want", "n't", "nt", "last", "take",
				"go", "get", "going", "long", "lot", "little", "first", "second", "third", "a", "about", "above",
				"after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at", "be",
				"because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot",
				"could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during",
				"each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having",
				"he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his",
				"how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's",
				"its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "n't",
				"of", "off", "on", "one", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out",
				"over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some",
				"such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there",
				"there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to",
				"too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were",
				"weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's",
				"whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're",
				"you've", "your", "yours", "yourself", "yourselves", "two", "three", "four", "five", "six", "seven",
				"eight", "nine", "ten", "na", "ago");
		for (String word : list) {
			stops.add(word);
		}

		try {
			File file2 = new File("./Ngrams/dict2.txt");
			File file3 = new File("./Ngrams/dict3.txt");
			BufferedReader in = new BufferedReader(new FileReader(file2));
			String line = "";
			while ((line = in.readLine()) != null) {
				bigramDict.add(line);
			}
			in.close();
			in = new BufferedReader(new FileReader(file3));
			while ((line = in.readLine()) != null) {
				trigramDict.add(line);
			}
			in.close();

		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			log.error(sw.toString());
		}
	}

	public int myPortNumber;
	public String myAddress;
	public Thread checker;
	public Configuration currJobConfig;
	public static String tempDir;
	public String workerIndex;
	public String masterAddr;
	public String inputFile;
	
	public static Map<String, Thread> checkers = new HashMap<>();
	
	private Hashtable<String, Integer> lexicon = new Hashtable<>();
	
	private Map<Integer, Set<Integer>> assignment = new HashMap<>();
	
	public SearchWorkerServer(int myPort, Map<String, String> config, String myAddr) {
			
		log.info("Creating server listener at socket " + myPort);
		currJobConfig = new Configuration();
		workerIndex = config.get("workerIndex");
		
		myPortNumber = myPort;
		setPort(myPort);
		myAddress = myAddr;
		masterAddr = config.get("master");
		
		tempDir = config.get("tempDir");    // db path
		File dir = new File(tempDir);
		dir.mkdirs();
		inputFile = tempDir + "/query.txt";
		   
		generateAssignment();
		
		Runnable messenger = new Runnable(){
			@Override
			public void run() {
				while (true) {		
					try {
						synchronized(this) {
							StringBuilder masterAddr = new StringBuilder(config.get("master"));	
							
							if (!masterAddr.toString().startsWith("http://")) {
								masterAddr = new StringBuilder("http://" + masterAddr.toString());
							}
							
							masterAddr.append("/querymulti/workerstatus");							
							
							try {								
								URL masterURL = new URL(masterAddr.toString());									
								HttpURLConnection conn = (HttpURLConnection)masterURL.openConnection();
								conn.setRequestProperty("Content-Type", "text/html");
								conn.addRequestProperty("Port", String.valueOf(myPort));
								conn.getResponseCode();
								
								StringBuilder builder = new StringBuilder();								
								log.info(builder.append("Worker check-in: ").append(conn.getResponseCode()).append(" ").append(conn.getResponseMessage()).toString()
										+ " with " + masterAddr);
							}
							catch (ConnectException e) {
								log.info("SearchWorkerServer cannot contact MasterServer");
							}							
							// worker checking in : 10s Interval
							wait(10000);								
						}						
					} 
					catch (InterruptedException e) {
						break;
					} 
					catch (Exception e) {
						StringWriter sw = new StringWriter();
						PrintWriter pw = new PrintWriter(sw);
						e.printStackTrace(pw);
						log.error(sw.toString()); // stack trace as a string
					}
				}
				log.info("SearchWorkerServer has stopped.");
			}
		};
		
		checker = new Thread(messenger);
		checkers.put(myAddr, checker);
		checker.start();
		
		final ObjectMapper om = new ObjectMapper();
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL); 

        Spark.post(new Route("/retrieve"){       	
        	@Override
        	public Object handle(Request arg0, Response arg1) {

        		String query = null;
				try {
					long time1 = System.currentTimeMillis();		
					query = (String) om.readValue(arg0.body(), HashMap.class).get("query");
					String workersList = (String) om.readValue(arg0.body(), HashMap.class).get("workersList");
					log.info("workerIndex -- > " + workerIndex);
					log.info("workersList -- > " + workersList);
	        		String[] queryList = query.split("\\s+");
	        		List<String> extractedWords = parseNgrams(queryList);
	        		for(String str : queryList) {
	        			if(stops.contains(str)) continue;
	        			extractedWords.add(str);
	        		}
	        		
	        		log.info("Query List ---->" + extractedWords.toString());
	        		
	        		int listSize = extractedWords.size();
	        		Hashtable<String, Hashtable<String, Double>> temp = new Hashtable<>();
	        		
	        		long startThread = System.currentTimeMillis();
	        		
	        		for (int i = 0; i < listSize; i++) { 
	        			String word = extractedWords.get(i);       // Should expend to Bigram and Trigram
	        			
	        			/* Fault Tolerance */
	        			if(!isMine(word, workersList, workerIndex)) continue;
	        			
	        			DataWork dataworker = new DataWork(word, temp, Integer.parseInt(workerIndex));
	        			dataworker.generateData();
	        		}
	        		
	        		long joinThread = System.currentTimeMillis();
	        		log.info("Thread working time ----> " + (joinThread - startThread) + "ms");
	        		
	        		long startWritingMap = System.currentTimeMillis();
	        		ObjectMapper mapper = new ObjectMapper();	        
			        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
					String m = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(temp);
					long endWritingMap = System.currentTimeMillis();
					log.info("WritingMap working time ----> " + (endWritingMap - startWritingMap) + "ms");
					
					long time2 = System.currentTimeMillis();
					log.info("Respond Total Time ----> " + (time2 - time1) + "ms");
					
	        		return m;
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
					log.error("Server Error!!!!");
				}
				return "Server Error";					
        	}
        });

        Spark.get(new Route("/shutdown") {

			@Override
			public Object handle(Request arg0, Response arg1) {	
				shutdown(myAddr);				
				System.exit(0);
				return "OK";
			}
		});
	}
	
	protected List<String> parseNgrams(String[] queryList) {
		List<String> res = new ArrayList<>();
		Matcher m;
		if (queryList == null || queryList.length == 0)
			return res;
		String prefix2 = "";
		String prefix3 = "";
		for (int i = 0; i < queryList.length; i++) {
			m = pan.matcher(queryList[i]);
			if (m.find()) {
				if (prefix2.length() != 0) {
					String candidate = prefix2 + " " + queryList[i];
					if (bigramDict.contains(candidate)) {
						res.add(candidate);
					}
				}
				prefix2 = queryList[i];
				if (prefix3.contains(" ")) {
					String candidate = prefix3 + " " + queryList[i];
					if (trigramDict.contains(candidate)) {
						res.add(candidate);
					}
					prefix3 = candidate.substring(candidate.indexOf(' ') + 1);
				} else {
					if (prefix3.length() != 0) {
						prefix3 += (" " + queryList[i]);
					} else {
						prefix3 = queryList[i];
					}
				}

			} else {
				prefix2 = "";
				prefix3 = "";
			}
		}
		return res;

	}

	private void generateAssignment() {
		for(int i = 0; i < 5; i++) {
			Set<Integer> set = getSet(i);
			assignment.put(i, set);
		}
	}
	
	public int getHashing(char ch){   // return hashing value to given character, converting into 0 - 26
		if(ch < 'a') return 0;
		else return ch - 'a' + 1;
	}
	
	public Set<Integer> getSet(int workerIndex) {
		Set<Integer> set = new HashSet<>();
		if(workerIndex == 0) {
			for(int i = 0; i <= 3; i++) set.add(i);
		}
		if(workerIndex == 1) {
			for(int i = 4; i <= 9; i++) set.add(i);
		}
		if(workerIndex == 2) {
			for(int i = 10; i <= 15; i++) set.add(i);
		}
		if(workerIndex == 3) {
			for(int i = 16; i <= 20; i++) set.add(i);
		}
		if(workerIndex == 4) {
			for(int i = 21; i <= 26; i++) set.add(i);
		}
		return set;
	}
	
	private boolean isMine(String word, String workersList, String workerIndex) {
		if(word == null || word.isEmpty()) return false;
		char ch = word.charAt(0);
		workersList = workersList.substring(1, workersList.length() - 1);
		String[] workers = workersList.split(", ");
		Set<Integer> index = new HashSet<>();
		for(int i = 0; i < workers.length; i++) {
			index.add(Integer.parseInt(workers[i].split(":")[1]) - 8000);    // indexes from 0 - 4
		}
		System.out.println(index);
		int myIndex = Integer.parseInt(workerIndex);
		int chInt = getHashing(ch);
		if(myIndex == 0) {
			if(index.contains(4)) {
				return assignment.get(myIndex).contains(chInt);
			} else {
				return assignment.get(myIndex).contains(chInt) || assignment.get(4).contains(chInt);
			}
		} else {
			if(index.contains(myIndex - 1)) {
				return assignment.get(myIndex).contains(chInt);
			} else {
				return assignment.get(myIndex).contains(chInt) || assignment.get(myIndex - 1).contains(chInt);
			}
		}
	}
	
	public static void createWorker(Map<String, String> config) throws FileNotFoundException {
		
		if (!config.containsKey("workerList"))
			throw new RuntimeException("Worker spout doesn't have list of worker IP addresses/ports");

		if (!config.containsKey("workerIndex"))
			throw new RuntimeException("Worker spout doesn't know its worker ID");
		
		else {
			
			String[] addresses = WorkerHelper.getWorkers(config);
			String myAddress = addresses[Integer.valueOf(config.get("workerIndex"))];				
			
			log.info("Initializing worker " + myAddress);			
			URL url;		
			try {				
				url = new URL(myAddress);
				new SearchWorkerServer(url.getPort(), config, myAddress);		
				
			} catch (MalformedURLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void shutdown(String addr) {
		shutdown();
		checkers.get(addr).interrupt();
		checkers.remove(addr);		
	}

	public static void shutdown() {		
		synchronized(topologies) {
			for (String topo: topologies)
				if (cluster != null) {
					cluster.killTopology(topo);
				}
		}
		if (cluster != null) {
			cluster.shutdown();
		}
	}
	
	
	/**
	 * Helper class for invoking CrawlerWorkerServer Node from ANT script
	 * @param args
	 * arg0: List of workers represented as a single string [IP_1, IP_2, ..., IP_N]
	 * arg1: Index of current server among the workers list
	 * arg2: IP Address of Master-Servlet
	 * arg3: Input Directory to read data from
	 * arg4: Output Directory to write data to
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException, IOException {
		
    	Properties props = new Properties();
    	props.load(new FileInputStream("./resources/log4j.properties"));
    	PropertyConfigurator.configure(props);
		
		if (args == null || args.length != 4) {
			System.err.println("Incorrect run-time arguments...");
			return;
		}
		
		Configuration conf = new Configuration();
		conf.put("workerList",  args[0]); // For testing, List like config.put("workerList", "[127.0.0.1:8001,127.0.0.1:8002]");
		conf.put("workerIndex", args[1]); // For testing, number of 0, 1
		conf.put("master",      args[2]); // For testing, "127.0.0.1:8080"
		conf.put("tempDir",     args[3]); // For testing, "127.0.0.1:8080"
				    
		SearchWorkerServer.createWorker(conf);		
	}
	

}
