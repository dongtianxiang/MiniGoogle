package edu.upenn.cis.stormlite.bolts.BuildGraph;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import edu.upenn.cis.stormlite.bolts.IRichBolt;
import edu.upenn.cis.stormlite.bolts.OutputCollector;
import edu.upenn.cis.stormlite.infrastructure.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.infrastructure.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.database.DBInstance;
import edu.upenn.cis455.database.DBManager;
import edu.upenn.cis455.database.Node;
import org.apache.log4j.Logger;

public class SecondaryReduceBolt implements IRichBolt {
	
	public static Logger log = Logger.getLogger(SecondaryReduceBolt.class);
	public Map<String, String> config;
    public String executorId = UUID.randomUUID().toString();
	public Fields schema = new Fields("key");
	public DBInstance graphDB;
	public DBInstance tempDB;
	public OutputCollector collector;
	public boolean sentEof = false;
	public String serverIndex;
	public File outfile;
	public PrintWriter outputWriter;
	public int eosNeeded;
	
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

	}

	@Override
	public void execute(Tuple input) {
		
    	if (sentEof) {   		
	        if (!input.isEndOfStream()) {
	        	throw new RuntimeException("We received data after we thought the stream had ended!");
	        }
		}
    	else if (input.isEndOfStream()) {
	        eosNeeded--;
	        if (eosNeeded == 0) {        	
	        	List<String> values = tempDB.getValues(executorId, "BUFFER");        	
	        	for (String value: values) {
	        		if (!graphDB.hasNode(value)) {
	        			Node newNode = new Node(value);
	        			outputWriter.println(newNode.getID());
	        			graphDB.addNode(newNode);
	        		}
	        	}
	        }
    	}
    	else {
    		
    		String key = input.getStringByField("key");
	        int written = Integer.parseInt(config.get("keysWritten"));
	        config.put("keysWritten", (new Integer(written + 1)).toString());        
	        log.info("Secondary reductor received: " + key); 	        
	        tempDB.addKeyValue(executorId, "BUFFER", key);
    	}
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
				
		config = stormConf;
		serverIndex = stormConf.get("workerIndex");		
		String graphDataDir = config.get("graphDataDir");
		String outputFileDir = config.get("outputDir");	
		String databaseDir   = config.get("databaseDir");
		
		if (serverIndex != null) {
			graphDataDir  += "/" + serverIndex;
			outputFileDir += "/" + serverIndex;
			databaseDir   += "/" + serverIndex + "-2";
		}
		
		File outfileDir = new File(outputFileDir);
		if (!outfileDir.exists()) {
			outfileDir.mkdirs();
		}
		
		String outputFileName = "dest.txt";
		outfile = new File(outfileDir, outputFileName);
		try {
			outputWriter = new PrintWriter(outfile);
		} 
		catch (FileNotFoundException e) {
			e.printStackTrace();
			return;
		}
		
		graphDB = DBManager.getDBInstance(graphDataDir);		
		DBManager.createDBInstance(databaseDir);
		tempDB  = DBManager.getDBInstance(databaseDir);
			
        this.collector = collector;
        if (!stormConf.containsKey("mapExecutors")) {
        	throw new RuntimeException("Reducer class doesn't know how many map bolt executors");
        }

	}

	@Override
	public void setRouter(StreamRouter router) {		
		// router has only to do with collector
		collector.setRouter(router);
	}

	@Override
	public Fields getSchema() {
		return schema;
	}

}
