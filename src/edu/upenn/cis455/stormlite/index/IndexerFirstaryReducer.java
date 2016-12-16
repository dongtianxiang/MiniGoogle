package edu.upenn.cis455.stormlite.index;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.bolts.IRichBolt;
import edu.upenn.cis.stormlite.bolts.OutputCollector;
import edu.upenn.cis.stormlite.bolts.bdb.FileWriterQueue;
import edu.upenn.cis.stormlite.bolts.bdb.GraphBuildFirstStageReducer;
import edu.upenn.cis.stormlite.infrastructure.Job;
import edu.upenn.cis.stormlite.infrastructure.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.infrastructure.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.database.DBInstance;
import edu.upenn.cis455.database.Node;

/**
 * @author Hany
 *	This stage is to calculate the total doc nums contains certain keyword
 */
public class IndexerFirstaryReducer implements IRichBolt {
	Logger log = Logger.getLogger(IndexerFirstaryReducer.class);
//	public static Logger log = Logger.getLogger(FirstaryReduceBolt.class);
	public Map<String, String> config;
    public String executorId = UUID.randomUUID().toString();
	public Fields schema = new Fields("url", "word"); 
	public OutputCollector collector;
	public Integer eosNeeded = 0;
//	public static DBInstance graphDB;
	public static DBInstance tempDB;
	public String serverIndex;
	public File outfile;
	public FileWriter outputWriter;
	private FileWriterQueue fwq;
	private int count = 0;
	private TopologyContext context;
	public static AtomicBoolean eosSent = new AtomicBoolean();
	
	
	public IndexerFirstaryReducer() {
		// TODO Auto-generated constructor stub
	}

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
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple input) {
    	if (eosSent.get()) { 
    		
	        if (!input.isEndOfStream()) {
	        	log.info("Server# " + serverIndex + "::"+executorId+" Firstary MAYDAY MAYDAY! " + input.getStringByField("word") + " / " + input.getStringByField("url"));
	        	log.error("We received data after we thought the stream had ended!");
	        	return;
	        }
	        log.error("Server# " + serverIndex + "::"+executorId+" Firstary MAYDAY MAYDAY! EOS AGAIN!!!!!");
		} else if (input.isEndOfStream()) {
    		eosNeeded--;
			log.info("Server#" + serverIndex + "::"+executorId+" EOS Received(reducer): " + (++count)+"/"+(eosNeeded+count));			
			
			if (eosNeeded == 0) {
				
				eosSent.set(true);
				
				log.info("start first-level reduction");				
				config.put("status", "REDUCING");				
				Map<String, List<String>> table;
				
				synchronized(tempDB) {
					table = tempDB.getTable(executorId);	
				}
				
				log.info("Server# " + serverIndex + " " + table);				
				Iterator<String> keyIt = table.keySet().iterator();	
				
				while (keyIt.hasNext()) {
					//key: word, value: urls 
					String key = keyIt.next();					
					List<String> values = table.get(key);
					fwq.addQueue(String.format("keyword: %s, DOC countS: %s\n", key, values.size()));
				}
					
				synchronized(tempDB) {
					tempDB.clearTempData();	
				}
				/*
				try {
					Thread.sleep(15);
				} catch (InterruptedException e) {
					StringWriter sw = new StringWriter();
					PrintWriter pw = new PrintWriter(sw);
					e.printStackTrace(pw);
					log.error(sw);
				}
				
				collector.emitEndOfStream();
				log.info("Server#"+serverIndex+"::"+executorId+" emits eos to reducer2.");
	        	*/
	        	if(fwq.queue.isEmpty()) {
	        		config.put("status", "IDLE");	  
	        	}
				
			}
    	}
    	else {
    		
    	
    		
    		
    		String word = input.getStringByField("word");
	        String url = input.getStringByField("url");	        	              
	        log.info("Server# " + serverIndex +"::"+executorId+ " Firstary reducer received: " + word + " / " + url);
	        synchronized(tempDB) {
	        	tempDB.addKeyValue(executorId, word, url);
	        }
    	}

	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		this.config = stormConf;
		this.context = context;
		this.collector = collector;
		outfile = new File(config.get("outputDir"), executorId);
		this.fwq = FileWriterQueue.getFileWriterQueueFromMap(outfile, context);
		
		int numMappers  = Integer.parseInt(stormConf.get("mapExecutors"));
		int numReducers = Integer.parseInt(stormConf.get("reduceExecutors"));
		int numWorkers  = Integer.parseInt(stormConf.get("workers"));	
        eosNeeded = (numWorkers - 1) * numMappers* numReducers  +  numMappers;	
        log.info("Num EOS required for ReduceBolt: " + eosNeeded);

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
