package edu.upenn.cis455.indexer.searchStore;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IndexerStoreDriver {
	
	private void store(String inputPath, String outputPath) {
		Configuration conf = new Configuration();
		try {
		    Job job = Job.getInstance(conf, "Indexer Storage");
		    job.setJarByClass(IndexerStoreDriver.class);
		    
		    MultithreadedMapper.setMapperClass(job, IndexerStoreMapper.class);
	        MultithreadedMapper.setNumberOfThreads(job, 10);
		    
	        job.setMapperClass(MultithreadedMapper.class);
	        
	        job.setCombinerClass(IndexerStoreReducer.class);
	        
		    job.setReducerClass(IndexerStoreReducer.class);

		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class); // changed
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);

		    FileInputFormat.addInputPath(job, new Path(inputPath));
		    FileOutputFormat.setOutputPath(job, new Path(outputPath));
		    
		    job.waitForCompletion(true);
		} catch (Exception e) {
		    e.printStackTrace();
		}
	   }

		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			String crawlInput = args[0];
			String output = args[1];
			
			IndexerStoreDriver cd = new IndexerStoreDriver();
			cd.store(crawlInput, output);
		}
}
