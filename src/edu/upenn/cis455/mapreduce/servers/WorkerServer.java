package edu.upenn.cis455.mapreduce.servers;

import static spark.Spark.setPort;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
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
import edu.upenn.cis455.database.DBInstance;
import spark.Request;
import spark.Response;
import spark.Route;
import spark.Spark;

/**
 * Simple listener for worker creation 
 */
public class WorkerServer {
	
	static Logger log = Logger.getLogger(WorkerServer.class);	
//	public static DistributedCluster cluster;
	
	public DistributedCluster cluster;
	
    List<TopologyContext> contexts = new ArrayList<>();
	static List<String> topologies = new ArrayList<>();
	
	public static DBInstance mapReduceTempStore;
	
	public int myPortNumber;
	public String myAddress;
	public Thread checker;
	public Configuration currJobConfig;
	public String tempStore;
	public String workerIndex;
	
	public static Map<String, Thread> checkers = new HashMap<>();
	
	public WorkerServer(int myPort, Map<String, String> config, String myAddr) 
			throws MalformedURLException, FileNotFoundException {
			
		log.info("Creating server listener at socket " + myPort);
		currJobConfig = new Configuration();
		workerIndex = config.get("workerIndex");
		myPortNumber = myPort;
		setPort(myPort);
		myAddress = myAddr;
		
		Runnable messenger = new Runnable(){
			@Override
			public void run() {
				while (true) {				
					StringBuilder masterAddr = new StringBuilder(config.get("master"));	
					try {
						synchronized(this) {
							
							if (!masterAddr.toString().startsWith("http://")) {
								masterAddr = new StringBuilder("http://" + masterAddr.toString());
							}
							String currJob     = currJobConfig.get("job");
							String keysRead    = currJobConfig.get("keysRead");
							String keysWritten = currJobConfig.get("keyWritten");
							String status      = currJobConfig.get("status");						
							if (status == null) status = "IDLE";							
							if (status.equals("IDLE") && cluster != null && cluster.running()) {
								cluster.shutdown();
							}							
							masterAddr.append("/workerstatus?");							
							masterAddr.append("port=" + myPort);								
							masterAddr.append("&job=" +         (currJob == null ?     "N/A" : currJob));																							
							masterAddr.append("&keysRead=" +    (keysRead == null ?    "N/A" : keysRead));						
							masterAddr.append("&keysWritten=" + (keysWritten == null ? "N/A" : keysRead));
							masterAddr.append("&status=" +      (status == null ?      "N/A" : status));							
							try {								
								URL masterURL = new URL(masterAddr.toString());									
								HttpURLConnection conn = (HttpURLConnection)masterURL.openConnection();
								conn.setRequestProperty("Content-Type", "text/html");
								StringBuilder builder = new StringBuilder();								
								log.debug(builder.append("Worker Status Report: ").append(conn.getResponseCode()).append(" ").append(conn.getResponseMessage()).toString());
							}
							catch (ConnectException e) {
								log.info("WorkerServer cannot contact MasterServer");
							}							
							// interval for background check
							wait(5000);								
						}						
					} 
					catch (InterruptedException e) {
						break;
					} 
					catch (IOException e) {
						e.printStackTrace();
					}
				}
				log.info("WorkerServer has stopped.");
			}
		};
		
		checker = new Thread(messenger);
		checkers.put(myAddr, checker);
		checker.start();
		
		final ObjectMapper om = new ObjectMapper();
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL); 
              
        Spark.post(new Route("/definejob") {

			@Override
			public Object handle(Request arg0, Response arg1) {
				
				try {
					synchronized(this) {
						wait(1000);
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				cluster = new DistributedCluster();
				WorkerJob workerJob = null;
				try {
					workerJob = om.readValue(arg0.body(), WorkerJob.class);
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				if (workerJob == null) throw new IllegalStateException();				
				
				Configuration config = workerJob.getConfig();
				currJobConfig = config;
				
				// other information about job should be already in configuration object
				String inputDirectory  = config.get("inputDir");	
				tempStore = config.get("databaseDir");
				
				config.put("keysRead",    "0");
				config.put("keysWritten", "0");
				config.put("databaseDir", tempStore);
				config.put("workerIndex", workerIndex);
				config.put("graphDataDir", "graphStore");
								
				File inDirTest  = (inputDirectory.equals("")) ? new File("./") : new File(inputDirectory);
				File outDirTest = (inputDirectory.equals("")) ? new File("./") : new File(inputDirectory);
				outDirTest.mkdirs();
				
				if (!inDirTest.isDirectory() || !outDirTest.isDirectory()) {
					log.info("Invalid user inputs. Job initialization failed.");
				}
	        	try {		        		
	        		log.info("Processing job definition request" + config.get("job") + " on machine " + config.get("workerIndex"));		        				        		
					synchronized (topologies) {
						contexts.add(cluster.submitTopology(config.get("job"), config, workerJob.getTopology()));	
						topologies.add(config.get("job"));
					}	
				} 
	        	catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
	            return "Job launched";
			}
        	
        });
        
        Spark.post(new Route("/runjob") {
			@Override
			public Object handle(Request arg0, Response arg1) {
        		log.info("Starting job!");
				cluster.startTopology();
				return "Started";
			}
        });
        
        Spark.post(new Route("/pushdata/:stream") {

			@Override
			public Object handle(Request arg0, Response arg1) {
				
				try {
					String stream = arg0.params(":stream");					
					Tuple tuple = om.readValue(arg0.body(), Tuple.class);					
					log.debug("Worker received: " + tuple + " for " + stream);					
					// Find the destination stream and route to it
					StreamRouter router = cluster.getStreamRouter(stream);					
					if (contexts.isEmpty()) {
						log.error("No topology context -- were we initialized??");			
					}
			    	if (tuple.isEndOfStream()) {
						router.executeEndOfStreamLocally(contexts.get(contexts.size() - 1));
			    	}
					else {
						router.executeLocally(tuple, contexts.get(contexts.size() - 1));
					}					
			    	return "OK";
				}
				catch (Exception e) {
					e.printStackTrace();					
					arg1.status(500);
					return e.getMessage();
				}

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
				new WorkerServer(url.getPort(), config, myAddress);		
				
			} catch (MalformedURLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void shutdown(String addr) {
		shutdown();
		checkers.get(addr).interrupt();
		checkers.remove(addr);		
	}

	public void shutdown() {		
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
	 * Helper class for invoking WorkerServer Node from ANT script
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
		conf.put("workerList",  args[0]);
		conf.put("workerIndex", args[1]);
		conf.put("master",      args[2]);
		conf.put("databaseDir", args[3]);
		
		WorkerServer.createWorker(conf);		
	}
}
