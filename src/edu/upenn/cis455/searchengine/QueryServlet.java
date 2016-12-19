package edu.upenn.cis455.searchengine;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.*;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.nlp.simple.Sentence;
import edu.upenn.cis.stormlite.infrastructure.Configuration;


@SuppressWarnings("serial")
public class QueryServlet extends HttpServlet {
	
	public static Logger log = Logger.getLogger(QueryServlet.class);
	final static int RETURNSIZE = 50;
	final static String STOPLIST = "./stopwords.txt";
	
	private Hashtable<String, Integer> stops = new Hashtable<>();
	
	@Override
	public void init(){
        File stop = new File(STOPLIST);
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
		
	public static HttpURLConnection sendJob(String dest, String reqType, Configuration config, String job, String parameters) throws IOException {			
	  	if (!dest.startsWith("http://")) {
	  		dest = "http://" + dest;
	  	}
	  
  		URL url = new URL(dest + "/" + job);		
  		log.info("Sending request to " + url.toString());
  		
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

	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
					IOException {
		/**
		 * Parse the query using Standford NLP simple API
		 * If stoplist words or numbers are found, append them to extra list
		 * Enable searching on stoplist
		 */
		String query = req.getParameter("query");
//		log.info("query:" + query);
		String queryList = "";
		String extraList = "";
		
		Pattern pan = Pattern.compile("[a-zA-Z0-9.@-]+");
		Pattern pan2 = Pattern.compile("[a-zA-Z]+");
		Pattern pan3 = Pattern.compile("[0-9]+,*[0-9]*");
		Matcher m, m2, m3;
		int querySize = 0;
		
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
							queryList += w + " ";
							querySize++;
						} else {
							// stop
							extraList += w + " ";
						}
					}
				} else {
					if (m3.matches()) {
						w = w.replaceAll(",", "");
						extraList += w + " ";
					}
				}
			}			
		}
		
		log.info("queryList:" + queryList);
		log.info("extraList:" + extraList);
		
		// TODO when queryList's results are not enough
		Hashtable<String, Hashtable<String, Double>> table = new Hashtable<>();	
		
		/**
		 * Retrieve data from all other worker servers		
		 */
		// retrieve info: send afterLemma to all workerServer
        Configuration config = new Configuration();             
        config.put("maxFileSize", "1000");       
        // convert afterLemma into JSON
        ObjectMapper mapper = new ObjectMapper();	        
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);  
				
		String[] workersList = new String[]{"127.0.0.1:8001", "127.0.0.1:8002"};    
	    config.put("workerList", Arrays.toString(workersList));	
	    
	    config.put("query", queryList);	  
	    
	    log.info(" ******* start retrieve data ******* ");
		try {
			int j = 0;
			int numberOfWorker = workersList.length;
			
			for (String dest: workersList) {		        
				config.put("workerIndex", String.valueOf(j++));		        
		        HttpURLConnection conn = QueryServlet.sendJob(dest, "POST", config, "retrieve", mapper.writerWithDefaultPrettyPrinter().writeValueAsString(config));
		        
		        if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
		        	log.info("Wrong with retrieve connection");
		        	return;
		        }
		        
				InputStream in = conn.getInputStream();
				BufferedReader br = new BufferedReader(new InputStreamReader(in));				
		        StringBuilder sb = new StringBuilder();
				String line = br.readLine();
				while (line != null && !line.isEmpty() ) {
					sb.append(line + "\n");
					line = br.readLine();
				}
				
				ObjectMapper om = new ObjectMapper();
		        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
		        
				@SuppressWarnings("unchecked")
				Hashtable<String, Hashtable<String, Double>> t  = om.readValue(sb.toString(), Hashtable.class);
				log.info("map: " + t);
				synchronized (table) {
					table.putAll(t);
					numberOfWorker--;
					
					if (numberOfWorker == 0) {	
						log.info(" ********* start computing ********** ");
						// reconstruct hashmap
						Hashtable<String, Hashtable<String, Double>> after = new Hashtable<>();
						for (String word: table.keySet()) {
							Hashtable<String, Double> wordList = table.get(word);
							for (String doc: wordList.keySet()) {
								double weight = wordList.get(doc);
								Hashtable<String, Double> ht = null;
								if (after.containsKey(doc)) {
									ht = after.get(doc);
								} else {
									ht = new Hashtable<String, Double>();
								}
								ht.put(word, weight);
								after.put(doc, ht);
							}							
						}
						
						// filter 
						Hashtable<String, Double> docList = new Hashtable<>();
						Hashtable<String, Double> extra = new Hashtable<>();
						for (String doc: after.keySet()) {
							Hashtable<String, Double> innertable = after.get(doc);
							if (innertable.size() == querySize) {
								// intersect
								double rank = 0;
								for (String w: innertable.keySet()) {
									rank += innertable.get(w);
								}
								docList.put(doc, rank);
							} else {
								// extra intersect
								double rank = 0;
								for (String w: innertable.keySet()) {
									rank += innertable.get(w);
								}
								extra.put(doc, rank);
							}
						}

				        Set<Entry<String, Double>> docListSet = docList.entrySet();
				        List<Entry<String, Double>> finallist = new ArrayList<Entry<String, Double>>(docListSet);
				        Collections.sort(finallist, new Comparator<Map.Entry<String, Double>>() {
				            public int compare(Map.Entry<String, Double> o1,
				                    Map.Entry<String, Double> o2) {
				                return  o2.getValue().compareTo(o1.getValue());
				            }
				        });
				        
				        // if returned pages are not enough
				        List<Entry<String, Double>> extralist = null;
						if (finallist.size() < RETURNSIZE) {
							Set<Entry<String, Double>> extraListSet = extra.entrySet();
					        extralist = new ArrayList<Entry<String, Double>>(extraListSet);
					        Collections.sort(extralist, new Comparator<Map.Entry<String, Double>>() {
					            public int compare(Map.Entry<String, Double> o1,
					                    Map.Entry<String, Double> o2) {
					                return  o2.getValue().compareTo(o1.getValue());
					            }
					        });
					        finallist.addAll(finallist.size(), extralist);
					        log.info("final: " + finallist);
					    }				        
						
						HttpSession s = req.getSession();	// add to session
						s.setAttribute("prev", 0);
						s.setAttribute("start", 0);
						s.setAttribute("finallist", finallist); 
						s.setAttribute("q", query);
						
						resp.sendRedirect("/result?query=" + query + "&start=0");

					}
				}
			}				
		}
		catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}
}
