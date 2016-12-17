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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.stanford.nlp.simple.Sentence;
import edu.upenn.cis.stormlite.infrastructure.Configuration;
import edu.upenn.cis.stormlite.infrastructure.Topology;
import edu.upenn.cis.stormlite.infrastructure.TopologyBuilder;
import edu.upenn.cis.stormlite.infrastructure.WorkerJob;
import edu.upenn.cis.stormlite.tuple.Fields;

@SuppressWarnings("serial")
public class QueryServlet extends HttpServlet {

	private Hashtable<String, Integer> stops = new Hashtable<>();
	public static Logger log = Logger.getLogger(QueryServlet.class);
	private static int count = 0;
	private Hashtable<String, List<String>> table;
			
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
		
		String path = req.getRequestURI();
		
		// computation
		if (path.equals("/query/intermediate")) {
			// get data
			InputStream in = req.getInputStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(in));

			ObjectMapper om = new ObjectMapper();
	        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);			
			
	        StringBuilder sb = new StringBuilder();
			String line = br.readLine();
			while (line != null && !line.isEmpty() ) {
				sb.append(line + "\n");
				line = br.readLine();
			}
//			Hashtable<String, List<String>> t = om.readValue(sb, java.util.Hashtable.class);
			
			
			count--;
			if (count == 0) {
				log.info(" ******* start computing ******** ");
				
				
				
				
				
				
			}			
			return;
		}
		
		// data retrieval
		else  {		
			String query = req.getParameter("searchquery");	
			String afterLemma = "";
			table = new Hashtable<>();
			
			//TODO number of machines
			count = 2;
			
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
								afterLemma += w + " ";
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

			// retrieve info: send afterLemma to all workerServer
	        Configuration config = new Configuration();             
	        config.put("maxFileSize", "1000");
	        
	        // convert afterLemma into JSON
	        ObjectMapper mapper = new ObjectMapper();	        
	        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);  
					
			String[] workersList = new String[]{"127.0.0.1:8001", "127.0.0.1:8002"};    
		    config.put("workerList", Arrays.toString(workersList));	
		    
		    config.put("query", afterLemma);
		    
		    log.info(" ******* start retrieve data ******* ");
			try {
				int j = 0;
				for (String dest: workersList) {
			        config.put("workerIndex", String.valueOf(j++));
					if (QueryServlet.sendJob(dest, "POST", config, "retrieve", mapper.writerWithDefaultPrettyPrinter().writeValueAsString(config)).getResponseCode() != HttpURLConnection.HTTP_OK) {					
						throw new RuntimeException("Job definition request failed");
					}
				}				
			}
			catch (JsonProcessingException e) {
				e.printStackTrace();
			}
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
		
	public static void distributeWorker(int numSpouts, int numMappers, int numReducers, String inputDir, String outputDir, String jobName) throws IOException {
		String SPOUT  = "SPOUT";
		String MAP_BOLT     = "MAP_BOLT";
		String REDUCE_BOLT  = "REDUCE_BOLT";
		String RESULT_BOLT  = "RESULT_BOLT";
			  
		SearchSpout  spout = new SearchSpout();
		SearchMapBolt    map  = new SearchMapBolt();
		SearchReduceBolt reducer = new SearchReduceBolt();
		SearchResultBolt result  = new SearchResultBolt();
			    
		// build topology
		TopologyBuilder builder = new TopologyBuilder();
			
		builder.setSpout(SPOUT,  spout,  numSpouts);
		builder.setBolt(MAP_BOLT,  map,  numMappers).shuffleGrouping(SPOUT);
		builder.setBolt(REDUCE_BOLT, reducer,  numReducers).fieldsGrouping(MAP_BOLT, new Fields("key"));
		builder.setBolt(RESULT_BOLT,  result,  1).firstGrouping(REDUCE_BOLT);
			
		Topology topo = builder.createTopology();	  
	        
        // create configuration object
        Configuration config = new Configuration(); 
        config.put("job", jobName);
        config.put("mapClass", jobName); 
        config.put("reduceClass", jobName);
        config.put("spoutExecutors", String.valueOf(numSpouts));
        config.put("mapExecutors", String.valueOf(numMappers));
        config.put("reduceExecutors", String.valueOf(numReducers));
        //TODO change it to dynamic
        config.put("workers", "2");		// hard code here       
        config.put("maxFileSize", "1000");
	        
        WorkerJob job = new WorkerJob(topo, config);
        ObjectMapper mapper = new ObjectMapper();	        
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);        
	   
		String[] workersList = new String[]{"127.0.0.1:8001", "127.0.0.1:8002"};    
	    config.put("workerList", Arrays.toString(workersList));		        
	        
		try {
			int j = 0;
			for (String dest: workersList) {
		        config.put("workerIndex", String.valueOf(j++));
				if (QueryServlet.sendJob(dest, "POST", config, "definejob", mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job)).getResponseCode() != HttpURLConnection.HTTP_OK) {					
					throw new RuntimeException("Job definition request failed");
				}
			}
			for (String dest: workersList) {
				if (QueryServlet.sendJob(dest, "POST", config, "runjob", "").getResponseCode() != HttpURLConnection.HTTP_OK) {						
					throw new RuntimeException("Job execution request failed");
				}
			}
			// TODO
			
			
			
		}
		catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	  }
	
	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
					IOException {

	}

}
