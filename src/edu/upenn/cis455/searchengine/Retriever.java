package edu.upenn.cis455.searchengine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Hashtable;
import java.io.*;

import org.apache.log4j.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.upenn.cis.stormlite.infrastructure.Configuration;

public class Retriever implements Runnable {
	
	public static Logger log = Logger.getLogger(Retriever.class);	
	private HttpURLConnection conn;
	private String dest, reqType, job, parameters;
	private Configuration config;
	private ObjectMapper mapper = new ObjectMapper();	
	private FileWriter fw;
      	
	public Retriever(FileWriter fw, String dest, String reqType, Configuration config, String job, String parameters) {
		this.dest = dest;
		this.reqType = reqType;
		this.config = config;
		this.job = job;
		this.parameters = parameters;
		mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
		this.fw = fw;
	}

	@Override
	public void run() {
		try {
			conn = sendJob(dest, reqType, config, job, parameters);
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
			
			@SuppressWarnings("unchecked")
			Hashtable<String, Hashtable<String, Double>> t  = mapper.readValue(sb.toString(), Hashtable.class);	
			synchronized(fw) {
				for (String w: t.keySet()) {
					Hashtable<String,Double> map = t.get(w);
					for (String d: map.keySet()) {
						double weight = map.get(d);
						String[] l = d.split(" ");
						String doc = l[0];
						String T = "F";
						if (l.length > 1) {
							T = "T";
						}
						fw.write(w + " " + doc + " " + weight + " " + T + "\n");
					}
				}
				fw.flush();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static HttpURLConnection sendJob(String dest, String reqType, Configuration config, String job, String parameters) throws IOException {			
	  	if (!dest.startsWith("http://")) {
	  		dest = "http://" + dest;
	  	}	  
  		URL url = new URL(dest + "/" + job);		
  		log.info("Now sending request to " + url.toString());
  		
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
	
}
