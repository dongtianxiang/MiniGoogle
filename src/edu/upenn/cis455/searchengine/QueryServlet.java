package edu.upenn.cis455.searchengine;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.*;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import edu.stanford.nlp.simple.Sentence;

@SuppressWarnings("serial")
public class QueryServlet extends HttpServlet {

	Hashtable<String, Integer> stops = new Hashtable<>();
	public static Logger log = Logger.getLogger(QueryServlet.class);
	
	File db = new File("./fakedb.txt");
	PrintWriter pw;
	
	@Override
	public void init(){
        File stop = new File("./stopwords.txt");
        try {
        	Scanner sc = new Scanner(stop);
        	while (sc.hasNext()) {
        		String s = sc.nextLine();
        		stops.put(s, 1);
        	}
        	sc.close();
        } catch (IOException e) {
        	e.printStackTrace();
        }
	}
	
	
	@Override
	public void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
    		IOException{
		File q = new File("./query.txt");
		pw = new PrintWriter(q);
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
						if ( !stops.containsKey(w)) {
							// not stop word
							Scanner sc = new Scanner(db);
							while (sc.hasNextLine()) {
								String line = sc.nextLine();
								String[] parts = line.split("=");
								if (parts[0].equalsIgnoreCase(w)) {
									pw.println(w + "->" + parts[1] + "->" + lemmas);
								}
							}
						} else {
							// stop
						}
					}
				} else {
					if (m3.matches()) {
						w = w.replaceAll(",", "");
					}
				}
			}			
		}
		
		pw.close();
		SearchComputeTopology sct = new SearchComputeTopology();
		try {
			sct.runMapreduce();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ServerSocket ss = new ServerSocket(8080);
		Socket s = ss.accept();
		InputStream in = s.getInputStream();
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String nextLine = br.readLine();
		while (nextLine != null && !nextLine.isEmpty()) {
			log.info(nextLine + "\n");
		}
	}

}
