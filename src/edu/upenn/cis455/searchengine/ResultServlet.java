package edu.upenn.cis455.searchengine;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.amazonaws.auth.AWSCredentials;
import edu.upenn.cis.stormlite.utils.AWSCredentialReader;


@SuppressWarnings("serial")
public class ResultServlet extends HttpServlet {
	
	public static Logger log = Logger.getLogger(ResultServlet.class);
	final static int PAGESIZE = 10;
	final static int sizeLimit = 100;
	
	public String error( ){
		return "No you are wrong";
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
    		IOException{		
		HttpSession se = req.getSession(false);
		if (se == null) {
			log.debug("Render result without computing result");
			error();
			return;
		}
		
		// should have a session attached
		String q = (String) se.getAttribute("q");
		int start = (int) se.getAttribute("start");	
		int prev = (int) se.getAttribute("prev");	
		List<Entry<String, Double>> finallist = (List<Entry<String, Double>>) req.getSession().getAttribute("finallist");
		
		log.info(" ******* retrieve pages from S3 ******** ");
		int listSize = finallist.size();
		List<Entry<String, Double>> retrieval;
		// if finallist is enough
		if (listSize - start >= PAGESIZE) {
			retrieval = new ArrayList<>(finallist.subList(start, start + PAGESIZE));
		} else {
			retrieval = new ArrayList<>(finallist.subList(start, listSize));
		}
		
		log.info("retrieval: " + retrieval);
		
		List<Hashtable<String, String>> pages = new ArrayList<Hashtable<String, String>>();
//		AWSCredentials credentials = AWSCredentialReader.getCredential();
		List<Thread> threads = new ArrayList<>(PAGESIZE);
		for (int i = 0; i < PAGESIZE; i++) {
			Entry<String,Double> m = retrieval.get(i);
			String url = m.getKey();
			Thread t = new Thread(new DataWorker(url, pages));
//			Thread t = new Thread(new DataWorker(url, credentials, pages));
			threads.add(t);
			t.start();
		}
		int tSize = threads.size();
		try {
    		for (int i = 0; i < tSize; i++) {
    			threads.get(i).join();
    		}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		log.info(" ********** pages preparation ends *********** ");
		
        log.info(" ******* start rendering result page ******* ");				        
        File f = new File("./html/FakeResult.html");
        Document resultPage = Jsoup.parse(f, "utf-8");
        resultPage.getElementById("placeholder").text(q);
                
        int s = pages.size();
        for (int i = 0; i < s; i++) {
        	Hashtable<String, String> map = pages.get(i);
        	String url = map.get("url");
        	if (!url.startsWith("http://") && !url.startsWith("https://")) {
        		url = "http://" + url;
        	}
        	resultPage.getElementById("t" + (i+1)).child(0).text(map.get("title"));
        	resultPage.getElementById("t" + (i+1)).child(0).attr("href", url);
        	resultPage.getElementById("u" + (i+1)).text(map.get("url"));
        	resultPage.getElementById("d" + (i+1)).text(map.get("desc"));
        }

//        resultPage.getElementById("p" + start / 10 + 1).addClass("disable");
//        resultPage.getElementById("p" + prev).removeClass("disable");
        
        PrintWriter pw = resp.getWriter();
        pw.println(resultPage.toString());
        pw.flush();
        
        log.info(" ********** clean up *********** ");
        pages.clear();
        
	}
	
}
