package edu.upenn.cis.stormlite.bolts;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.UUID;
import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.infrastructure.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.infrastructure.TopologyContext;
import edu.upenn.cis.stormlite.infrastructure.WorkerHelper;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;

/**
 * A trivial bolt that simply outputs its input stream to the
 * console
 * 
 * @author zives
 *
 */
public class PrintBolt implements IRichBolt {
	static Logger log = Logger.getLogger(PrintBolt.class);
	
	Fields myFields = new Fields();
	
	int eosRequired = 0;
	
	Map<String, String> config;

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the PrintBolt, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    
    public String outputDirectory;
    public File targetDirectory;
    public File targetFile;
    public PrintWriter pw;    
    int eosReceived = 0;  

    public PrintBolt() {
    	
    }
    
    public void removeOutputDirectory() {
    	outputDirectory = null;
    	targetDirectory = null;
    }

	@Override
	public void cleanup() {
		// Do nothing
	}

	@Override
	public void execute(Tuple input) {
		
		if (!input.isEndOfStream()) {	
			
			String key = input.getStringByField("key");
			String val = input.getStringByField("value");			
			String output = key + "," + val;				
			log.info(output);
			pw.println(output);
			pw.flush();			
		}
		else {
			eosReceived++;
			eosRequired--;
			if (eosRequired == 0) {
				log.info("******** Map-Reduce job completed! *********");
				
//				WorkerServer.status = "idle";
				// TODO
				
				pw.close();
			}			
			log.debug("EOS Receved: " + eosReceived);
		}		
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		
		int numMappers = Integer.parseInt(stormConf.get("mapExecutors"));	
		int numSpouts = Integer.parseInt(stormConf.get("spoutExecutors"));	
		int numReducers = Integer.parseInt(stormConf.get("reduceExecutors"));
				
		String[] workers = WorkerHelper.getWorkers(stormConf);				
		int numWorkers = workers.length;
		int M = ((numWorkers - 1) * numMappers  + 1) * numSpouts;		
        int N = M * numReducers * (numWorkers - 1) * numMappers + M * numMappers;       
        this.eosRequired = numWorkers * numReducers * N;
        log.info("Num EOS required for PrintBolt: " + this.eosRequired);           
        config = stormConf;             
        outputDirectory = config.get("outputDir");       
        targetDirectory = new File(outputDirectory);
        
        if (!targetDirectory.exists()) {
        	targetDirectory.mkdirs();
        }
        
        targetFile = new File(targetDirectory, "output.txt");
        
        try {       	
        	System.out.println(targetFile.getAbsolutePath());       	
			pw = new PrintWriter(targetFile);
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
        
	}

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void setRouter(StreamRouter router) {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(myFields);
	}

	@Override
	public Fields getSchema() {
		return myFields;
	}

}
