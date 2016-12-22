package edu.upenn.cis455.indexer.hadoop;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.TimeZone;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.log4j.Logger;

public class IndexerHadoopReducer extends Reducer<Text, Text, Text, Text> {
    private static final Logger log = Logger.getLogger(IndexerHadoopReducer.class);
	protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
		Iterator<Text> iter = values.iterator();
		int count = 0;
		
//	    Date date = new Date();
//	    SimpleDateFormat sdf = new SimpleDateFormat("E yyyy.MM.dd 'at' hh:mm:ss a zzz");
//	    sdf.setTimeZone(TimeZone.getTimeZone("EST"));
//	    String nowDate = sdf.format(date);
		log.info(": Reducer ----> " + key);
		
		while (iter.hasNext()) {
			count++;
			Text reducerText = new Text(iter.next() + " " + count);
		    context.write(key, reducerText);
		}
    }
}