package edu.upenn.cis455.searchengine;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.bolts.IRichBolt;
import edu.upenn.cis.stormlite.bolts.OutputCollector;
import edu.upenn.cis.stormlite.bolts.pagerank.PRResultBolt;
import edu.upenn.cis.stormlite.infrastructure.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.infrastructure.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.database.Node;

public class SearchResultBolt implements IRichBolt{
	
	public static Logger log = Logger.getLogger(SearchResultBolt.class);
	public String executorId = UUID.randomUUID().toString();
	public Fields schema     = new Fields();
	public int eosRequired = 0;  
    public int eosReceived = 0;
    public static Map<String, String> config;    
    private Hashtable<String, String> docList = new Hashtable<>();

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		docList.clear();
	}

	@Override
	public void execute(Tuple input) {
		if (!input.isEndOfStream()) {	
			String key = input.getStringByField("key");
			String weight = input.getStringByField("value");
			int index = key.indexOf(":");
			String doc = key.substring(0, index);
			docList.put(doc, weight);
		}
		else {
			eosReceived++;
			if (eosRequired == eosReceived) {			
				log.info("*** Finish computing *");
				log.info(docList.toString());
				
				// Send HTTP Request to result servlet
				try {
					URL myURL = new URL("http://localhost:8080");					
					HttpURLConnection conn = (HttpURLConnection) myURL.openConnection();
					conn.setRequestMethod("POST");					
					OutputStream out = conn.getOutputStream();
					log.info("making http request");
				    DataOutputStream wr = new DataOutputStream(out);
				    for (String key: docList.keySet()) {
				    	String weight = docList.get(key);
				    	wr.write((key + ":" + weight).getBytes());
				    }
				    
				    
				} catch (Exception e) {
					// url failed
				}
				
				config.put("status", "IDLE");
				cleanup();
			}
			log.debug("EOS Receved: " + eosReceived);
		}
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		int numMappers = Integer.parseInt(stormConf.get("mapExecutors"));	
		int numSpouts = Integer.parseInt(stormConf.get("spoutExecutors"));	
		int numReducers = Integer.parseInt(stormConf.get("reduceExecutors"));			
		int numWorkers = Integer.parseInt(stormConf.get("workers"));		
		int M = ((numWorkers - 1) * numMappers + 1) * numSpouts;		
        int N = M * numReducers * (numWorkers - 1) * numMappers + M * numMappers; 
        
        eosRequired = numWorkers * numReducers * N;
        log.info("Num EOS required for Result Bolt: " + this.eosRequired); 

        config = stormConf;        
	}

	@Override
	public void setRouter(StreamRouter router) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Fields getSchema() {
		// TODO Auto-generated method stub
		return schema;
	}

}
