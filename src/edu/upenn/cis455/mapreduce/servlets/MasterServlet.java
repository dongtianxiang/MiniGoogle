package edu.upenn.cis455.mapreduce.servlets;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.bolts.BuildGraph.BuilderMapBolt;
import edu.upenn.cis.stormlite.bolts.BuildGraph.BuilderStoreBolt;
import edu.upenn.cis.stormlite.bolts.PageRank.PRMapBolt;
import edu.upenn.cis.stormlite.bolts.PageRank.PRReduceBolt;
import edu.upenn.cis.stormlite.bolts.PageRank.PRResultBolt;
import edu.upenn.cis.stormlite.infrastructure.Configuration;
import edu.upenn.cis.stormlite.infrastructure.Topology;
import edu.upenn.cis.stormlite.infrastructure.TopologyBuilder;
import edu.upenn.cis.stormlite.infrastructure.WorkerJob;
import edu.upenn.cis.stormlite.spouts.LocalDBBuilder.LinksFileSpout;
import edu.upenn.cis.stormlite.spouts.LocalDBBuilder.LinksSpout;
import edu.upenn.cis.stormlite.spouts.PageRank.RankFileSpout;
import edu.upenn.cis.stormlite.spouts.PageRank.RankSpout;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis455.mapreduce.servers.WorkerInformation;

public class MasterServlet extends HttpServlet {

  static final long serialVersionUID = 455555001;
  private static final String SPOUT  = "LINK_SPOUT";
  private static final String MAP_BOLT = "MAP_BOLT";
  private static final String REDUCE_BOLT = "REDUCE_BOLT";
  private static final String RESULT_BOLT  = "RES_BOLT";
  private static final Integer totalWorkers = 2;
  private static Integer availableWorkers = 0;
  
  // note that this it's statically initiated
  static Map<String, WorkerInformation> workers;  
  static Thread checkerThread;
  
  public static Configuration globalConf = null;
  public static int count = 0;
 
  static {
	  
	  workers = new TreeMap<>();
	  Runnable r = new Runnable() {
		  
		  @Override
		  public void run() {
			  
			  while (true) {
				  
				  List<String> list = new LinkedList<>();
				  
						for (String key: workers.keySet()) {							
							Long time = (new Date()).getTime();							
							WorkerInformation info = workers.get(key);
							Long lastCheckIn = info.lastCheckIn;							
							if (time > lastCheckIn + 10000) {
								list.add(key);
							}
							
						}
						for (String key: list) {
							workers.remove(key);
						}
						
						// TODO: add back after experiment is completed
//						if (availableWorkers == totalWorkers) {
//							  Runnable r = new Runnable() {  
//								  @Override
//								  public void run() {
//									  try {
//										  synchronized(this) {
//											  wait(2000);
//										  }											  
//										  try {
//											distributeJob("edu.upenn.cis455.mapreduce.job.WordCount", 1, 1, "data2", "out2", "JOB1");
//											
//										  } catch (IOException e) {
//											e.printStackTrace();
//										}										 									  
//									  } catch (InterruptedException e) {
//										  e.printStackTrace();
//									  }
//								  }
//							  };
//							  
//							  Thread t = new Thread(r);
//							  t.start();
//						  }
						
						
				  synchronized(this) {					  					  
						try {
							wait(25000);
						} catch (InterruptedException e) {
							break;
						}
				  } 
				  
			  }
			  System.out.println("Thread terminated");
		  }
	  };
	  
	  checkerThread = new Thread(r);	  
	  checkerThread.start(); 
	  
  }

  public void doGet(HttpServletRequest request, HttpServletResponse response) 
       throws java.io.IOException {
	  
	  String URI = request.getRequestURI();	  
	  	  
	  /* worker check in */
	  if (URI.startsWith("/workerstatus")) {
	
		  String port = request.getParameter("port");
		  String status = request.getParameter("status");
		  String job = request.getParameter("job");
		  String keysRead = request.getParameter("keysRead");
		  String keysWritten = request.getParameter("keysWritten");
		  		  		  
		  Date date = new Date();		  
		  String remoteHost = request.getRemoteHost() + ":" + port;	
		  WorkerInformation workerStatus = new WorkerInformation();
		  
		  try {			  
			  
			  workerStatus.currentJob = job;
			  workerStatus.keysRead = keysRead;
			  workerStatus.keysWritten = keysWritten;			  
			  workerStatus.IPAddress = remoteHost;
			  workerStatus.status = status;
			  workerStatus.lastCheckIn = date.getTime();
			  
			 synchronized(availableWorkers) {
				 if (status.equals("idle") && availableWorkers < totalWorkers) {
					 availableWorkers += 1;
				 }
			 }
			 
//			 System.out.println(availableWorkers);
			  
			  synchronized(workers) {
				  workers.put(remoteHost, workerStatus);
			  }
	
		  } catch (Exception e) {
			  e.printStackTrace();
			  response.setStatus(400);
			  return;
		  }		  
  
		  System.out.println("Port:" + port);
		  System.out.println("Status: " + status);
		  System.out.println("Job: " + job);
		  System.out.println("KeysRead: " + keysRead);
		  System.out.println("keysWritten: " + keysWritten);		  
		  System.out.println("----------------------------------");
		  
		  if (port        == null || 
			  status      == null || 
			  job         == null || 
			  keysRead    == null || 
			  keysWritten == null ) {
			  
			  response.setStatus(400);
			  response.setContentType("text/html");
			  PrintWriter out = response.getWriter();			  
			  out.println("<html>");
			  out.println("<h1>");
			  out.println("400 BAD REQUEST");
			  out.println("</h1>");
		      out.println("</html>");   
		      
			  return;
		  }
		  
		  response.setContentType("text/html");
		  PrintWriter out = response.getWriter();
		  out.println("<html><head><title>Worker Status</title></head>");
		  out.println("<body>");
		  
		  out.println("Port:" + port);
		  out.println("Status: " + status);
		  out.println("Job: " + job);
		  out.println("KeysRead: " + keysRead);
		  out.println("keysWritten: " + keysWritten);
		  
		  out.println("</body>");
	      out.println("</html>");         
	  }
	  else if (URI.startsWith("/status")) {
		  
		  PrintWriter out = response.getWriter();
		  
		  out.println("<div>");
		  
			  out.println("<h3 style=\"color: blue\"> Worker Status </h3>");
			  out.println("<p>");
			  out.println("<table style=\"width:90%\">");
			  
			  out.println("<tr>");
			  out.println("<th>");  out.println(" IP "); out.println("</th>");
			  out.println("<th>");  out.println(" Job ");  out.println("</th>");
			  out.println("<th>");  out.println(" Status "); out.println("</th>");
			  out.println("<th>");  out.println(" Keys Read "); out.println("</th>");
			  out.println("<th>");  out.println(" Keys Written "); out.println("</th>");			  
			  out.println("<th>");  out.println(" Job Class "); out.println("</th>");			  
		      out.println("</tr>");		
		      
			  for (String workerID: workers.keySet()) {			  
				  
				  out.println("<tr>");			  
				  WorkerInformation info = workers.get(workerID);				  			  
				  out.println("<th>");  out.println(info.IPAddress); out.println("</th>");
				  out.println("<th>");  out.println(info.currentJob);  out.println("</th>");
				  out.println("<th>");  out.println(info.status); out.println("</th>");		
				  out.println("<th>");  out.println(info.keysRead); out.println("</th>");
				  out.println("<th>");  out.println(info.keysWritten); out.println("</th>");
				  if (globalConf != null) {				  
					  out.println("<th>");  out.println(globalConf.get("mapClass")); out.println("</th>");
				  } else {
					  out.println("<th>");  out.println("N/A"); out.println("</th>"); 
				  }
				  out.println("</tr>");				  
			  }
	  
			  out.println("/<table>");		  
			  out.println("</p>");		  
	      out.println("</div>");
	      
	      out.println("<div>");
	      
			  out.println("<h3 style=\"color: blue\"> Submit Job </h3>");	      
			  out.println("<p>");
			  out.println("<form method=\"GET\" action=\"create\">");
			  
		  out.println("</div>");			  
		  out.println("<div>");		  	 		  
			  // class name
			  out.println("Job Class Name: ");
			  out.println("<input type=\"text\" name=\"className\" value=\"\"> <br>");		  		  
		  out.println("</div>");		  
		  out.println("<div>");		  
 		  
		  // class name
			  out.println("Job Name: ");
			  out.println("<input type=\"text\" name=\"jobName\" value=\"\"> <br>");		  	  
		  out.println("</div>");
		  out.println("<div>");
		  // input directory
		  out.println("Input Directory: ");
		  out.println("<input type=\"text\" name=\"inputDir\" value=\"\"> <br>");		  
		  out.println("</div>");		
		  
		  out.println("<div>");
		  // output directory
		  out.println("Output Directory: ");
		  out.println("<input type=\"text\" name=\"outputDir\" value=\"\"> <br>");		  
		  out.println("</div>");
		  
		  out.println("<div>");
		  // number of map threads
		  out.println("Number of Mappers: ");
		  out.println("<input type=\"text\" name=\"numMappers\" value=\"\"> <br>");		  
		  out.println("</div>");
		  
		  out.println("<div>");
		  // number of reducer threads
		  out.println("Number of Reducers: ");
		  out.println("<input type=\"text\" name=\"numReducers\" value=\"\"> <br>");	  		  
		  out.println("</div>");
		  
		  // submit button
		  out.println("<div>");
		  out.println("<input type=\"submit\" value=\"submit\"><br><br>");
		  out.println("</form>");		  		  
	      out.println("</div>");		  
	      out.println("</html>");
  
	  }
	  else if (URI.startsWith("/create")) {
		  
			String jobClass = request.getParameter("className");
			String inputDir = request.getParameter("inputDir");
			
			// forward restoration
			if (inputDir == null) {			
				inputDir = "";
			}
			
			String outputDir = request.getParameter("outputDir");
			
			if (outputDir == null) {
				outputDir = "";
			}
			
			String jobName = request.getParameter("jobName");
			Integer numMappers = null, numReducers = null;
			
			try {	
				numMappers  = Integer.parseInt(request.getParameter("numMappers"));
				numReducers = Integer.parseInt(request.getParameter("numReducers"));				
									
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			if (jobClass == null || inputDir == null || outputDir == null || numMappers == null || numReducers == null) {
				
				// invalid input
			}
			else {
				distributePRJob(numMappers, numReducers, inputDir, outputDir, jobName);
			}
			
			response.sendRedirect("status");
	  }
	  
	  else if ( URI.startsWith("/shutdown")) {
		  
		  checkerThread.interrupt();
		  for(String workerIP: workers.keySet()) {
			  
			  try {
				  WorkerInformation info = workers.get(workerIP);			  
				  System.out.println("shutting down " + info.IPAddress);			  
				  String destAddr = "http://" + info.IPAddress;			  
				  URL url = new URL(destAddr+ "/shutdown");
				  HttpURLConnection conn = (HttpURLConnection)url.openConnection();
				  conn.setRequestProperty("Content-Type", "text/html");			  
				  conn.getResponseCode();
				  conn.getResponseMessage();
			  } catch (Exception e) {
				  // ignore
			  }
		  }
		  
		  response.setStatus(200);		  
		  Runnable ender = new Runnable() {
			  @Override
			  public void run() {			  
				  synchronized(this) {
					  try {
						  wait(1000);
					  } catch (InterruptedException e) {
						  // ignore
					  }
					  System.exit(0);
				  }			  
			  }
		  };
		  
		  Thread t = new Thread(ender);
		  t.start(); 
		  PrintWriter pw = response.getWriter();
		  pw.println("Server Closed");		  
		  pw.flush();
		  return;
	  }
	  else {
		  response.sendError(404);
	  }
  }
  
  public static HttpURLConnection sendJob(String dest, String reqType, Configuration config, String job, String parameters) throws IOException {
		
	  	if (!dest.startsWith("http://")) {
	  		dest = "http://" + dest;
	  	}
	  
  		URL url = new URL(dest + "/" + job);		
		System.out.println("Sending request to " + url.toString());		
		HttpURLConnection conn = (HttpURLConnection)url.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod(reqType);
		
		if (reqType.equals("POST")) {
			
			conn.setRequestProperty("Content-Type", "application/json");	
			OutputStream os = conn.getOutputStream();
			byte[] toSend = parameters.getBytes();
			os.write(toSend);
			os.flush();			
		} 
		else {
			conn.getOutputStream();
		}
		
		return conn;
  }
  
  public static void distributeBuildDistributedDB(int numMappers, int numReducers, String inputDir, String outputDir, String jobName) throws IOException {
	  
		LinksFileSpout spout     = new LinksSpout();		
	    BuilderMapBolt mapBolt   = new BuilderMapBolt();
	    BuilderStoreBolt reduceBolt = new BuilderStoreBolt();
	    
	    int numSpouts = 1;
	    String jobClass = "edu.upenn.cis455.mapreduce.jobs.PageRankJob";
	    
	    // build topology
		TopologyBuilder builder = new TopologyBuilder();			    			    
        builder.setSpout(SPOUT, spout, numSpouts);
        builder.setBolt(MAP_BOLT, mapBolt, numMappers).fieldsGrouping(SPOUT, new Fields("key"));		        
        builder.setBolt(REDUCE_BOLT, reduceBolt, numReducers).fieldsGrouping(MAP_BOLT, new Fields("key"));
        Topology topo = builder.createTopology();
        
        // create configuration object
        Configuration config = new Configuration();        
        config.put("mapClass", jobClass);
        config.put("reduceClass", jobClass);
        config.put("spoutExecutors",  (new Integer(numSpouts)).toString());
        config.put("mapExecutors",    (new Integer(numMappers)).toString());
        config.put("reduceExecutors", (new Integer(numReducers)).toString());
        config.put("inputDir", inputDir);
        config.put("outputDir", outputDir);
        config.put("job", jobName);       
        config.put("workers", "2");
        config.put("graphDataDir", "graphStore");
        config.put("databaseDir" , "storage");
        
        
        
        WorkerJob job = new WorkerJob(topo, config);
        ObjectMapper mapper = new ObjectMapper();	        
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);        
        String[] workersList = new String[]{"127.0.0.1:8000", "127.0.0.1:8001"};          
        config.put("workerList", Arrays.toString(workersList));		        
        
		try {
			int j = 0;
			for (String dest: workersList) {
		        config.put("workerIndex", String.valueOf(j++));
				if (MasterServlet.sendJob(dest, "POST", config, "definejob", mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job)).getResponseCode() != HttpURLConnection.HTTP_OK) {					
					throw new RuntimeException("Job definition request failed");
				}
			}
			for (String dest: workersList) {
				if (MasterServlet.sendJob(dest, "POST", config, "runjob", "").getResponseCode() != HttpURLConnection.HTTP_OK) {						
					throw new RuntimeException("Job execution request failed");
				}
			}
		}
		catch (JsonProcessingException e) {
			e.printStackTrace();
		}
  }
  
  
  public static void distributePRJob(int numMappers, int numReducers, String inputDir, String outputDir, String jobName) throws IOException {
	  
	  	int numSpouts = 1;  
	  	
	  	String jobClass = "edu.upenn.cis455.mapreduce.jobs.PageRankJob";
		RankFileSpout spout = new RankSpout();		
	    PRMapBolt mapBolt = new PRMapBolt();
	    PRReduceBolt reduceBolt = new PRReduceBolt();
	    PRResultBolt printer = new PRResultBolt();
	    
	    // build topology
		TopologyBuilder builder = new TopologyBuilder();			    			    
        builder.setSpout(SPOUT, spout, 1);
        builder.setBolt(MAP_BOLT, mapBolt, numMappers).fieldsGrouping(SPOUT, new Fields("value"));		        
        builder.setBolt(REDUCE_BOLT, reduceBolt, numReducers).fieldsGrouping(MAP_BOLT, new Fields("key"));
        builder.setBolt(RESULT_BOLT, printer, 1).shuffleGrouping(REDUCE_BOLT);		        
        Topology topo = builder.createTopology();
        
        // create configuration object
        Configuration config = new Configuration();        
        config.put("mapClass", jobClass);
        config.put("reduceClass", jobClass);
        config.put("spoutExecutors",  (new Integer(numSpouts)).toString());
        config.put("mapExecutors",    (new Integer(numMappers)).toString());
        config.put("reduceExecutors", (new Integer(numReducers)).toString());
        config.put("inputDir", inputDir);
        config.put("outputDir", outputDir);
        config.put("job", jobName);
        config.put("workers", "2");       
        config.put("decayFactor", "0.85");
        
        config.put("graphDataDir", "graphStore");
        config.put("databaseDir" , "storage");
        config.put("serverDataDir", "");
        
        WorkerJob job = new WorkerJob(topo, config);
        ObjectMapper mapper = new ObjectMapper();	        
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        
        String[] workersList = new String[]{"127.0.0.1:8000", "127.0.0.1:8001"};       
        config.put("workerList", Arrays.toString(workersList));		        
        
		try {
			int j = 0;
			for (String dest: workersList) {
		        config.put("workerIndex", String.valueOf(j++));
				if (MasterServlet.sendJob(dest, "POST", config, "definejob", mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job)).getResponseCode() != HttpURLConnection.HTTP_OK) {					
					throw new RuntimeException("Job definition request failed");
				}
			}
			for (String dest: workersList) {
				if (MasterServlet.sendJob(dest, "POST", config, "runjob", "").getResponseCode() != HttpURLConnection.HTTP_OK) {						
					throw new RuntimeException("Job execution request failed");
				}
			}
		}
		catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}
  
  
  public void doPost(HttpServletRequest request, HttpServletResponse response)  {
	 
  }
  
}
  
