package edu.upenn.cis455.stormlite.searchengine;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.bolts.IRichBolt;
import edu.upenn.cis.stormlite.bolts.OutputCollector;
import edu.upenn.cis.stormlite.bolts.pagerank.PRReduceBolt;
import edu.upenn.cis.stormlite.infrastructure.Job;
import edu.upenn.cis.stormlite.infrastructure.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.infrastructure.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.database.DBManager;

public class SearchReduceBolt implements IRichBolt{
	
	public static Logger log = Logger.getLogger(SearchReduceBolt.class);
	
	public static Map<String, String> config;
    public String executorId = UUID.randomUUID().toString();
	public Fields schema = new Fields("key", "value"); 
	public Job reduceJob;
	public OutputCollector collector;
	public Integer eosNeeded = 0;
	public boolean sentEof = false;
	public int count = 0;
	private String lemmas = null;
	private Hashtable<String, List<String>> temp;
	
	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(schema);
	}

	@Override
	public void cleanup() {
		temp.clear();
		lemmas = null;
	}

	@Override
	public void execute(Tuple input) {
		try {		
	    	if (sentEof) {   		
		        if (!input.isEndOfStream()) {
		        	throw new RuntimeException("We received data after we thought the stream had ended!");
		        }
			}
	    	else if (input.isEndOfStream()) {	
//				log.info("EOS Received: " + (++count));			
				eosNeeded--;
				if (eosNeeded == 0) {					
					for (String doc: temp.keySet()) {
						List<String> list = temp.get(doc);
						reduceJob.reduce(doc + ":" + lemmas, list.iterator(), collector);
					}
					sentEof = true;
				}
				Thread.sleep(10);
				collector.emitEndOfStream();
	    	} else {
	    		// key: doc; value: word:weight:[lemmas]
	    		log.info("reducer receives key:" + input.getStringByField("key") + " value:" + input.getStringByField("value"));
	    		String doc = input.getStringByField("key");
	    		String[] pairs = input.getStringByField("value").split(":");
	    		if (count == 0) {
	    			int index = pairs[2].indexOf("]");
	    			lemmas = pairs[2].substring(1, index);
	    		}
	    		String word = pairs[0];
	    		List<String> list;
	    		if (!temp.containsKey(doc)) {
	    			list = new ArrayList<String>();
	    		} else {
	    			list = temp.get(doc);
	    		}
	    		list.add(word + ":" + pairs[1]);
	    		temp.put(doc, list);
	    	}		
	    	
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		config = stormConf;

        this.collector = collector;
        if (!stormConf.containsKey("reduceClass")) {
        	throw new RuntimeException("Mapper class is not specified as a config option");
        }
        else {        	
        	String mapperClass = stormConf.get("reduceClass");        	
        	try {
				reduceJob = (Job)Class.forName(mapperClass).newInstance();
			} 
        	catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				e.printStackTrace();
				throw new RuntimeException("Unable to instantiate the class " + mapperClass);
			}
        }
        
        if (!stormConf.containsKey("mapExecutors")) {
        	throw new RuntimeException("Reducer class doesn't know how many map bolt executors");
        }

		int numMappers  = Integer.parseInt(stormConf.get("mapExecutors"));	
		int numSpouts   = Integer.parseInt(stormConf.get("spoutExecutors"));	
		int numReducers = Integer.parseInt(stormConf.get("reduceExecutors"));
		int numWorkers  = Integer.parseInt(stormConf.get("workers"));
		
		int M = ((numWorkers - 1) * numMappers  + 1) * numSpouts;		
        eosNeeded = M * numReducers * (numWorkers - 1) * numMappers + M * numMappers;
        log.info("Num EOS required for ReduceBolt: " + eosNeeded);
        
        temp = new Hashtable<>();
	}

	@Override
	public void setRouter(StreamRouter router) {
		collector.setRouter(router);
	}

	@Override
	public Fields getSchema() {
		return schema;
	}

}
