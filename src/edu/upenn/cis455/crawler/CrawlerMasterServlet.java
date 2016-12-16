package edu.upenn.cis455.crawler;

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

import edu.upenn.cis.stormlite.infrastructure.Configuration;
import edu.upenn.cis.stormlite.infrastructure.Topology;
import edu.upenn.cis.stormlite.infrastructure.TopologyBuilder;
import edu.upenn.cis.stormlite.infrastructure.WorkerJob;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis455.crawler.bolts.CrawlerBolt;
import edu.upenn.cis455.crawler.bolts.DownloadBolt;
import edu.upenn.cis455.crawler.bolts.FilterBolt;
import edu.upenn.cis455.crawler.bolts.RecordBolt;
import edu.upenn.cis455.crawler.bolts.URLSpout;

public class CrawlerMasterServlet extends HttpServlet {

  static final long serialVersionUID = 455555001;
//  private static final String SPOUT  = "URL_SPOUT";
//  private static final String MAP_BOLT = "MAP_BOLT";
//  private static final String REDUCE_BOLT = "REDUCE_BOLT";
//  private static final String RESULT_BOLT  = "RES_BOLT";
  
  // note that this it's statically initiated
  static Map<String, CrawlerWorkerInformation> workers;  
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
							CrawlerWorkerInformation info = workers.get(key);
							Long lastCheckIn = info.lastCheckIn;							
							if (time > lastCheckIn + 30000) {    // decide as offline if no checked in over 30 s
								list.add(key);
							}
							
						}
						for (String key: list) {
							workers.remove(key);
						}
						
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
		  String keysRead = request.getParameter("keysRead");
		  String keysWritten = request.getParameter("keysWritten");
		  		  		  
		  Date date = new Date();		  
		  String remoteHost = request.getRemoteHost() + ":" + port;	
		  CrawlerWorkerInformation workerStatus = new CrawlerWorkerInformation();
		  
		  try {			  
			  
			  workerStatus.keysRead = keysRead;
			  workerStatus.keysWritten = keysWritten;			  
			  workerStatus.IPAddress = remoteHost;
			  workerStatus.lastCheckIn = date.getTime();
			  
			  synchronized(workers) {
				  workers.put(remoteHost, workerStatus);
			  }
	
		  } catch (Exception e) {
			  e.printStackTrace();
			  response.setStatus(400);
			  return;
		  }		  
  
		  System.out.println("Port:" + port);
		  System.out.println("VisitedURLs: " + keysRead);
		  System.out.println("Crawled Pages: " + keysWritten);		  
		  System.out.println("----------------------------------");
		  
		  if (port        == null || 
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
			  out.println("<th>");  out.println(" Visited URLs "); out.println("</th>");
			  out.println("<th>");  out.println(" Pages Crawled "); out.println("</th>");			  
			  out.println("<th>");  out.println(" Job Class "); out.println("</th>");			  
		      out.println("</tr>");		
		      
			  for (String workerID: workers.keySet()) {			  
				  
				  out.println("<tr>");			  
				  CrawlerWorkerInformation info = workers.get(workerID);				  			  
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
		  
		  // submit button
		  out.println("<div>");
		  out.println("<input type=\"submit\" value=\"submit\"><br><br>");
		  out.println("</form>");		  		  
	      out.println("</div>");		  
	      out.println("</html>");
  
	  }
	  else if (URI.startsWith("/create")) {

			distributeWorker();
			
			response.sendRedirect("status");
	  }
	  
	  else if ( URI.startsWith("/shutdown")) {
		  
		  checkerThread.interrupt();
		  for(String workerIP: workers.keySet()) {
			  
			  try {
				  CrawlerWorkerInformation info = workers.get(workerIP);			  
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
  
  public static void distributeWorker() throws IOException {
		String URL_SPOUT = "URL_SPOUT";
		String CRAWLER_BOLT = "CRAWLER_BOLT";
		String FILTER_BOLT = "FILTER_BOLT";
		String RECORD_BOLT = "RECORD_BOLT";
		  
		URLSpout spout = new URLSpout();
		CrawlerBolt boltA = new CrawlerBolt();
		DownloadBolt boltB = new DownloadBolt();
		FilterBolt boltD = new FilterBolt();
		RecordBolt boltE = new RecordBolt();
		    
		// build topology
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(URL_SPOUT, spout, 3);
		  
		builder.setBolt(CRAWLER_BOLT, boltA, 10).fieldsGrouping(URL_SPOUT, new Fields("url"));    
		builder.setBolt(FILTER_BOLT, boltD, 10).fieldsGrouping(CRAWLER_BOLT, new Fields("url"));
		builder.setBolt(RECORD_BOLT, boltE, 10).fieldsGrouping(FILTER_BOLT, new Fields("extractedLink"));
		
		Topology topo = builder.createTopology();	  
        
        // create configuration object
        Configuration config = new Configuration();           
        config.put("seedURL", "https://www.facebook.com/");
        config.put("maxFileSize", "20");
        
        WorkerJob job = new WorkerJob(topo, config);
        ObjectMapper mapper = new ObjectMapper();	        
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);        
        
        String[] workersList = new String[]{"172.31.60.134:8000", "172.31.52.13:8001", "172.31.16.183:8002", "172.31.55.47:8003", "172.31.25.146:8004"};   
//        String[] workersList = new String[]{"127.0.0.1:8000", "127.0.0.1:8001"};    	
        config.put("workerList", Arrays.toString(workersList));		        
        
		try {
			int j = 0;
			for (String dest: workersList) {
		        config.put("workerIndex", String.valueOf(j++));
				if (CrawlerMasterServlet.sendJob(dest, "POST", config, "definejob", mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job)).getResponseCode() != HttpURLConnection.HTTP_OK) {					
					throw new RuntimeException("Job definition request failed");
				}
			}
			for (String dest: workersList) {
				if (CrawlerMasterServlet.sendJob(dest, "POST", config, "runjob", "").getResponseCode() != HttpURLConnection.HTTP_OK) {						
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
  

