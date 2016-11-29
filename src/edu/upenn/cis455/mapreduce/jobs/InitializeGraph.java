package edu.upenn.cis455.mapreduce.jobs;

import java.util.Iterator;

import edu.upenn.cis.stormlite.infrastructure.Context;
import edu.upenn.cis.stormlite.infrastructure.Job;

public class InitializeGraph implements Job {

	@Override
	public void map(String key, String value, Context context) {
		
	}

	@Override
	public void reduce(String key, Iterator<String> values, Context context) {
		
	}

}
