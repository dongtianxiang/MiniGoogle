package edu.upenn.cis455.stormlite.index;

import java.io.File;
import java.io.FileWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.bolts.IRichBolt;
import edu.upenn.cis.stormlite.bolts.OutputCollector;
import edu.upenn.cis.stormlite.bolts.bdb.FileWriterQueue;
import edu.upenn.cis.stormlite.infrastructure.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.infrastructure.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.database.DBInstance;
import edu.upenn.cis455.database.Node;

public class IndexerSecondaryReducer implements IRichBolt {
	Logger log = Logger.getLogger(IndexerFirstaryReducer.class);
	public Map<String, String> config;
    public String executorId = UUID.randomUUID().toString();
	public Fields schema = new Fields("url", "word"); 
	public OutputCollector collector;
	public Integer eosNeeded = 0;
	public static DBInstance tempDB;
	public String serverIndex;
	public File outfile;
	public FileWriter outputWriter;
	private FileWriterQueue fwq;
	private int count = 0;
	private TopologyContext context;
	public static AtomicBoolean eosSent;
	public IndexerSecondaryReducer() {
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
	        	log.error("Server# " + serverIndex + "::"+executorId+" Firstary MAYDAY MAYDAY! " + input.getStringByField("url") + " / " + input.getStringByField("word"));
	        	log.error("We received data after we thought the stream had ended!");
	        	return;
//	        	throw new RuntimeException("We received data after we thought the stream had ended!");
	        }
	        log.error("Server# " + serverIndex + "::"+executorId+" Firstary MAYDAY MAYDAY! EOS AGAIN!!!!!");
		}
    	else if (input.isEndOfStream()) {
    		eosNeeded -= 1;
    		log.info("Server#" + serverIndex + "::"+executorId+"EOS Received(reducer2): " + (++count)+"/"+(eosNeeded+count));			
	        
	        if (eosNeeded == 0) {
	        	eosSent.set(true);
	        	log.info("start second level reduction");	
	        	config.put("status", "REDUCING2");
	        	Map<String, List<String>> table;
	        	synchronized(tempDB) {
	        		table = tempDB.getTable(executorId);
	        	}
	        	
	        	log.info(table.toString());
	        	
	        	Iterator<String> iter = table.keySet().iterator();
	        	while (iter.hasNext()) {
	        		
	        		String dest = iter.next();        		
	        		
		        		fwq.addQueue(String.format("%s\n",));
		        		
//		        		try {
//							outputWriter.write(String.format("%s\n", node.getID()));
//							outputWriter.flush();
//						} catch (IOException e) {
//							StringWriter sw = new StringWriter();
//							PrintWriter pw = new PrintWriter(sw);
//							e.printStackTrace(pw);
//							log.error(sw.toString());
//						}
		        		graphDB.addNode(node);	
 		
	        	}
	        	
	        	synchronized(tempDB) {
	        		tempDB.clearTempData();
	        	}
	        	if(FileWriterQueue.getFileWriterQueueLaterCall().queue.isEmpty()) {
	        		config.put("status", "IDLE");	  
	        	}
//		        log.info("************** Secondary reducer job completed! ******************");
	        }

	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		this.config = stormConf;
		this.context = context;
		this.collector = collector;

		serverIndex = stormConf.get("workerIndex");		
		String outputFileDir = config.get("outputDir");	
		String numWorkers = config.get("workers");
		
		outfile = new File(config.get("outputDir"), executorId);
		this.fwq = FileWriterQueue.getFileWriterQueue(outfile, context);
		
		int reducerNum = Integer.parseInt(stormConf.get("reduceExecutors"));
		int reducer2Num = Integer.parseInt(stormConf.get("reduce2Executors"));
		int workerNums = Integer.parseInt(numWorkers);	
		eosNeeded = (workerNums - 1) * reducerNum * reducer2Num + reducerNum;
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
