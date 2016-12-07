package edu.upenn.cis.stormlite.bolts.bdb;

import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.bolts.IRichBolt;
import edu.upenn.cis.stormlite.bolts.OutputCollector;
import edu.upenn.cis.stormlite.infrastructure.Job;
import edu.upenn.cis.stormlite.infrastructure.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.infrastructure.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;

public class BuilderMapBolt implements IRichBolt {

	public static Logger log = Logger.getLogger(BuilderMapBolt.class);
	public Map<String, String> config;
    public String executorId = UUID.randomUUID().toString();
	public Fields schema = new Fields("key", "value"); 
	public Job mapJob;
	public OutputCollector collector;
	public Integer eosNeeded = 0;
	public double d;	
	public String serverIndex = null;
	
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
		// do nothing
	}

	@Override
	public void execute(Tuple input) {
		
		if (!input.isEndOfStream()) {
			
			String src = input.getStringByField("key");				
			String[] links = input.getStringByField("value").split(", ");			
			for (String link: links) {
				collector.emit(new Values<Object>(src, link));
			}
		}
		else {
    		eosNeeded--;    		
    		if (eosNeeded == 0) {
    			log.info("Mapping phase completed");	   
        		collector.emitEndOfStream();
    		}
		}
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;
        this.config = stormConf;
        
        log.info(this.config);
        log.info("********** Start of mapping phase *********");       
        stormConf.put("status", "Mapping");
        
        if (!stormConf.containsKey("spoutExecutors")) {
        	throw new RuntimeException("Mapper class doesn't know how many input spout executors");
        }
        
//		int numMappers = Integer.parseInt(stormConf.get("mapExecutors"));	
//		int numSpouts  = Integer.parseInt(stormConf.get("spoutExecutors"));		
		int numWorkers = Integer.parseInt(stormConf.get("workers"));
//        eosNeeded = ((numWorkers - 1) * numMappers  + 1) * numSpouts;	
		
		eosNeeded = numWorkers;
		
		
        log.info("Num EOS required for MapBolt: " + eosNeeded);
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
