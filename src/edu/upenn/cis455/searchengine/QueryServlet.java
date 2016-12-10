package edu.upenn.cis455.searchengine;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.stanford.nlp.simple.Sentence;

public class QueryServlet extends HttpServlet {
	
//	static ConcurrentLinkedQueue<String> theQ = new ConcurrentLinkedQueue<String>();
	static ExecutorService executor;
	@Override
	public void init(){
        executor = Executors.newFixedThreadPool(5);
	}
	
	
	@Override
	public void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
    		IOException{
		String query = req.getParameter("searchquery");		
		Pattern pan = Pattern.compile("[a-zA-Z0-9.@-]+");
		Pattern pan2 = Pattern.compile("[a-zA-Z]+");
		Pattern pan3 = Pattern.compile("[0-9]+,*[0-9]*");
		Matcher m, m2, m3;
		// parse query
		Sentence sen = new Sentence(query);
		List<String> lemmas = sen.lemmas();		
		for (String w: lemmas) {
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
					}
				} else {
					if (m3.matches()) {
						w = w.replaceAll(",", "");
					}	
				}
			}
			System.out.println("Now start computing query...");
		}
	
		resp.setStatus(HttpServletResponse.SC_OK);
		resp.setContentType("text/html");
        PrintWriter pw = resp.getWriter();
        pw.println("Query:" + query);
        pw.flush();
	}

}