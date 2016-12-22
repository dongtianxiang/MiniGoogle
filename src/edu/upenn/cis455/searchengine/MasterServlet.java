package edu.upenn.cis455.searchengine;

import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.infrastructure.Configuration;
import edu.upenn.cis455.crawler.CrawlerWorkerInformation;

@SuppressWarnings("serial")
public class MasterServlet extends HttpServlet {
	
	public static Logger log = Logger.getLogger(MasterServlet.class);
	
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
							wait(1000);
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
		if (URI.equals("/workerstatus")) {

			String port = request.getHeader("Port");
			
			Date date = new Date();		  
			String remoteHost = request.getRemoteHost() + ":" + port;	
			CrawlerWorkerInformation workerStatus = new CrawlerWorkerInformation();

			try {			  	  
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

			if (port == null) {
				response.setStatus(400);
				return;
			}
			
			response.setStatus(200);
 			return;
		} 
		else if (URI.equals("/status")) {
			  
			PrintWriter out = response.getWriter();

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

}
