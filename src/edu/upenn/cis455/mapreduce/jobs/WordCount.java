package edu.upenn.cis455.mapreduce.jobs;
import java.util.Iterator;

import edu.upenn.cis.stormlite.infrastructure.Context;
import edu.upenn.cis.stormlite.infrastructure.Job;

public class WordCount implements Job {

  public void map(String key, String value, Context context) {
	  
	  // emit key:1 pair
	  context.write(value, "1");
  }
  
  public void reduce(String key, Iterator<String> values, Context context) {
	  
	  // Your reduce function for WordCount goes here
	  int total = 0;
	  
	  while (values.hasNext()) {
		  total += Integer.parseInt(values.next());
	  }
	  
	  context.write(key, total + "");
  }
  
}
