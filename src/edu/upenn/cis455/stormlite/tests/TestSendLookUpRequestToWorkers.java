package edu.upenn.cis455.stormlite.tests;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class TestSendLookUpRequestToWorkers {
	
	public static void main(String[] args) throws IOException {
		
		// hard-code dest for testing purpose
		String[] workers = new String[]{"127.0.0.1:8000", "127.0.0.1:8001"};
		
		for (String dest: workers) {
		
		  	if (!dest.startsWith("http://")) { dest = "http://" + dest; }
		  
	  		URL url = new URL(dest + "/lookupURL?word1=Amazon&word2=Times");		
	  		
			System.out.println("Sending request to " + url.toString());		
			
			HttpURLConnection conn;
			try {
				conn = (HttpURLConnection) url.openConnection();
				conn.setRequestMethod("GET");
				conn.setDoOutput(true);				
				conn.setRequestProperty("Content-Type", "text/html");			  
				
				int code = conn.getResponseCode();
				String msg = conn.getResponseMessage();
				
				InputStream instream = conn.getInputStream();
				
				BufferedReader bufReader = new BufferedReader(new InputStreamReader(instream));
				
				
				while (true) {
					
					String line = bufReader.readLine();
					
					if (line == null) break;
					
					System.out.println(line);
					
				}
			
				System.out.println(msg);
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		
		}
	}

}
