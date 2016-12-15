package edu.upenn.cis455.mapreduce.jobs;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.bolts.pagerank.PRReduceBolt;
import edu.upenn.cis.stormlite.infrastructure.Context;
import edu.upenn.cis.stormlite.infrastructure.Job;

public class SearchJob implements Job {
	
	public static Logger log = Logger.getLogger(SearchJob.class);

	@Override
	public void map(String key, String value, Context context) {
		// TODO Auto-generated method stub
		context.write(key, value);		
	}

	@Override
	public void reduce(String key, Iterator<String> values, Context context) {
		// TODO Auto-generated method stub
		String[] list = key.split(":");
		int length = list[1].length();
		String query = key.substring(1, length - 1);
		String[] lemmas = query.split(",");
		int count = 0;
		double weight = 0;
		while (values.hasNext()) {
			String v = values.next();
			String[] vList = v.split(":");
			double wordWeight = Double.parseDouble(vList[1]);
			weight += wordWeight;
			count++;
		}
		if (count == list.length) {
			context.write(key, String.valueOf(weight));
		}
	}

}
