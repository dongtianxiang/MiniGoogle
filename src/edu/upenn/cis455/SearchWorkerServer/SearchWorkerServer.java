package edu.upenn.cis455.SearchWorkerServer;

import static spark.Spark.setPort;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.infrastructure.Configuration;
import edu.upenn.cis.stormlite.infrastructure.DistributedCluster;
import edu.upenn.cis.stormlite.infrastructure.TopologyContext;
import edu.upenn.cis.stormlite.infrastructure.WorkerHelper;
import edu.upenn.cis.stormlite.infrastructure.WorkerJob;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.searchengine.DataWork;
import spark.Request;
import spark.Response;
import spark.Route;
import spark.Spark;

public class SearchWorkerServer {
	
	static Logger log = Logger.getLogger(SearchWorkerServer.class);	
	public static DistributedCluster cluster;
	
    List<TopologyContext> contexts = new ArrayList<>();
	static List<String> topologies = new ArrayList<>();
	
	public int myPortNumber;
	public String myAddress;
	public Thread checker;
	public Configuration currJobConfig;
	public String tempDir;
	public String workerIndex;
	public String masterAddr;
	public String inputFile;
	
	public static Map<String, Thread> checkers = new HashMap<>();
	
	private Hashtable<String, Integer> lexicon = new Hashtable<>();
	
	public SearchWorkerServer(int myPort, Map<String, String> config, String myAddr) {
			
		log.info("Creating server listener at socket " + myPort);
		currJobConfig = new Configuration();
		workerIndex = config.get("workerIndex");
		
		myPortNumber = myPort;
		setPort(myPort);
		myAddress = myAddr;
		masterAddr = config.get("master");
		
		tempDir = config.get("tempDir");
		File dir = new File(tempDir);
		dir.mkdirs();
		inputFile = tempDir + "/query.txt";
 
        // TODO: construct in-memory lexicon here 
		if (workerIndex.equals("1")) {
			lexicon.put("apple", 1);
		} else {
			lexicon.put("company", 1);
		}
		      		
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
							
							masterAddr.append("/workerstatus?");							
							masterAddr.append("port=" + myPort);
							
							try {								
								URL masterURL = new URL(masterAddr.toString());									
								HttpURLConnection conn = (HttpURLConnection)masterURL.openConnection();
								conn.setRequestProperty("Content-Type", "text/html");
								StringBuilder builder = new StringBuilder();								
								log.info(builder.append("Worker check-in: ").append(conn.getResponseCode()).append(" ").append(conn.getResponseMessage()).toString()
										+ " with " + masterAddr);
							}
							catch (ConnectException e) {
								log.info("SearchWorkerServer cannot contact MasterServer");
							}							
							// TODO interval for background check： 0.1s
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
					query = (String) om.readValue(arg0.body(), HashMap.class).get("query");
	        		String[] queryList = query.split(" ");
	        		int listSize = queryList.length;
	        		ArrayList<Thread> threads = new ArrayList<>();
	        		Hashtable<String, Hashtable<String, Double>> temp = new Hashtable<>();
	        		
	        		for (int i = 0; i < listSize; i++) {
	        			String word = queryList[i];
	        			if (!lexicon.containsKey(word)) {
	        				continue;
	        			}
	        			Thread t = new Thread(new DataWork(word, temp, Integer.parseInt(workerIndex)));
	        			threads.add(t);
	        			t.start();
	        		}
	        		
	        		int threadSize = threads.size();
	        		try {
	            		for (int i = 0; i < threadSize; i++) {
	            			threads.get(i).join();
	            		}
	        		} catch (InterruptedException e) {
	        			e.printStackTrace();
	        		}
	        		
					ObjectMapper mapper = new ObjectMapper();	        
			        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
					String m = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(temp);
	        		return m;
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
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
		conf.put("tempDir", args[3]); 	  // The location where mapreduce reads from
				    
		SearchWorkerServer.createWorker(conf);		
	}
	

}
