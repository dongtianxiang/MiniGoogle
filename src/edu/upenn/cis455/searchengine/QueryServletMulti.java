package edu.upenn.cis455.searchengine;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.stanford.nlp.simple.Sentence;
import edu.upenn.cis.stormlite.infrastructure.Configuration;
import edu.upenn.cis455.crawler.CrawlerWorkerInformation;
import scala.Tuple2;
import scala.Tuple3;

@SuppressWarnings("serial")
public class QueryServletMulti extends HttpServlet {
	
	public static Logger log = Logger.getLogger(QueryServletMulti.class);
	
	final static int RETURNSIZE = 50;
	final static String MAXFILESIZE = "1000";
	final static String STOPLIST = "./resources/stopwords.txt";
	
//	private Hashtable<String, Integer> stops = new Hashtable<>();
	private SparkWrapper spark;
	
	@Override
	public void init(){
//        File stop = new File(STOPLIST);
//        try {
//        	Scanner sc = new Scanner(stop);
//        	while (sc.hasNext()) {
//        		String s = sc.nextLine();
//        		stops.put(s, 1);
//        	}
//        	sc.close();
//        } catch (IOException e) {
//        	e.printStackTrace();
//        }
        // prepare spark
        spark = new SparkWrapper();
        spark.init();
	}
	
	static Map<String, CrawlerWorkerInformation> workers;  
	static Thread checkerThread;
	
	static {			
		workers = new HashMap<>();
		Runnable r = new Runnable() {
			@Override
			public void run() {
				while (true) {
					List<String> list = new LinkedList<>();
					for (String key: workers.keySet()) {
						Long time = (new Date()).getTime();							
						CrawlerWorkerInformation info = workers.get(key);
						Long lastCheckIn = info.lastCheckIn;							
						if (time > lastCheckIn + 15000) {    // decide as offline if no checked in over 30 s
							list.add(key);
						}
					}
					for (String key: list) {
						workers.remove(key);
					}				
					synchronized(this) {					  					  
						try {
							wait(5000);
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
		
	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
					IOException {
		
		String URI = req.getRequestURI();
		
		/* worker check in */
		if (URI.equals("/querymulti/workerstatus")) {
			String port = req.getHeader("Port");
			log.info("receive: " + req.getRemoteAddr() + ": " + port);
			
			Date date = new Date();		  
			String remoteHost = req.getRemoteHost() + ":" + port;	
			CrawlerWorkerInformation workerStatus = new CrawlerWorkerInformation();

			try {			  	  
				workerStatus.IPAddress = remoteHost;
				workerStatus.lastCheckIn = date.getTime();

				synchronized(workers) {
					workers.put(remoteHost, workerStatus);
				}
			} catch (Exception e) {
				e.printStackTrace();
				resp.setStatus(400);
				return;
			}		  

			if (port == null) {
				resp.setStatus(400);
				return;
			}
			
			resp.setStatus(200);
 			return;
 			
		} else if (URI.equals("/querymulti/status")) {
			  
			PrintWriter out = resp.getWriter();

			out.println("<div>");

			out.println("<h3 style=\"color: blue\"> Worker Status </h3>");
			out.println("<p>");
			out.println("<table style=\"width:90%\">");

			out.println("<tr>");
			out.println("<th>");  out.println(" IP "); out.println("</th>");	  
			out.println("</tr>");		

			for (String workerID: workers.keySet()) {			  

				out.println("<tr>");			  
				CrawlerWorkerInformation info = workers.get(workerID);				  			  
				out.println("<th>");  out.println(info.IPAddress); out.println("</th>");
				out.println("</tr>");				  
			}

			out.println("/<table>");		  
			out.println("</p>");		  
			out.println("</div>");	
			return;
			
		} else {
			/**
			 * Parse the query using Standford NLP simple API
			 * If stoplist words or numbers are found, append them to extra list
			 * Enable searching on stoplist
			 */ 
			String originalQuery = req.getParameter("query");
			String query = originalQuery;
			String queryList = "";

//			Pattern pan = Pattern.compile("[a-zA-Z0-9.@-]+");
			Pattern pan2 = Pattern.compile("[a-zA-Z]+");
			Pattern pan3 = Pattern.compile("[0-9]+,*[0-9]*");
			Pattern pan = Pattern.compile("^[a-zA-Z0-9]+[.@&-]*[a-zA-Z0-9]+");
			Matcher m, m2, m3;
			int querySize = 0;
			
			Sentence sen = new Sentence(query);
			List<String> lemmas = sen.lemmas();
			for (String w: lemmas) {
				w = w.trim();	// trim
				m = pan.matcher(w);
				m2 = pan2.matcher(w);
				m3 = pan3.matcher(w);
//				m4 = pan4.matcher(w);
				if (m.matches()) {
					if (m2.find()){			
						if (!w.equalsIgnoreCase("-rsb-")&&!w.equalsIgnoreCase("-lsb-")
								&&!w.equalsIgnoreCase("-lrb-")&&!w.equalsIgnoreCase("-rrb-")
								&&!w.equalsIgnoreCase("-lcb-")&&!w.equalsIgnoreCase("-rcb-")){
							w = w.toLowerCase();
//							if ( !stops.containsKey(w)) {
								// not stop word
								w = w.toLowerCase();
								queryList += w + " ";
								querySize++;
//							} 
						}			
					}
				}
			}
			log.info("queryList:" + queryList);
				
			/**
			 * Retrieve data from all other worker servers		
			 */
			log.info(" ******* start retrieve data ******* ");
			ArrayList<Thread> threads = new ArrayList<>();
	        Configuration config = new Configuration();             
	        config.put("maxFileSize", MAXFILESIZE);       
	        ObjectMapper mapper = new ObjectMapper();	        
	        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);  
				
	        // TODO configure all workers
//			String[] workersList = new String[]{"127.0.0.1:8001", "127.0.0.1:8002"};
//	        String[] workersList = new String[]{"158.130.214.88:8000"};
	        Set<String> worker = workers.keySet();       
	        String[] workersList = worker.toArray(new String[0]);
	        log.info("workerList: " + workersList);
		    config.put("workersList", Arrays.toString(workersList));		    
		    config.put("query", queryList);

			int j = 0;
			String user = req.getRemoteAddr();
			File folder = new File("./query/" + user);
			if (!folder.exists()) {
				folder.mkdirs();
			}
			File f = new File("./query/" + user + "/out.txt");
			if (f.exists()) {
				f.delete();
			}
			FileWriter fw = new FileWriter("./query/" + user + "/out.txt", true);
			for (String dest: workersList) {		        
				config.put("workerIndex", String.valueOf(j++));	
				Retriever r = new Retriever(fw, dest, "POST", config, "retrieve", mapper.writerWithDefaultPrettyPrinter().writeValueAsString(config));
			    Thread t = new Thread(r);
			    threads.add(t);
			    t.start();
			}    		
			try {
				int threadSize = threads.size();
				for (int i = 0; i < threadSize; i++) {
					threads.get(i).join();
				}
				fw.close();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
			log.info(" ********* start computing ********** ");	
			long t1 = System.currentTimeMillis();
			List<Tuple3<String, List<String>, Double>> list = spark.startSearchCount("./query/" + user + "/out.txt");
			long t2 = System.currentTimeMillis();
					
			log.info("time consuming: " + (t2 - t1));		        					
			HttpSession s = req.getSession();	// add to session
			s.setAttribute("finallist", list); 
			s.setAttribute("q", originalQuery);
			
			resp.sendRedirect("/resultmulti?query=" + query + "&start=0");
		}

	}						
}
