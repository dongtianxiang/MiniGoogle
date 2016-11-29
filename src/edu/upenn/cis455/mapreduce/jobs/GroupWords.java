package edu.upenn.cis455.mapreduce.jobs;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import edu.upenn.cis.stormlite.infrastructure.Context;
import edu.upenn.cis.stormlite.infrastructure.Job;

public class GroupWords implements Job {
	
	@Override
	public void map(String key, String value, Context context) {
		context.write(key, value);
	}

	@Override
	public void reduce(String key, Iterator<String> values, Context context) {
		
		List<String> list = new LinkedList<>();
		
		while (values.hasNext()) {			
			list.add(values.next());
		}
		
		context.write(key, list.toString());
	}
}
