package edu.upenn.cis455.indexer.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

public class IndexerHadoopDriver {

	private void indexer(String inputPath, String outputPath) {
		Configuration conf = new Configuration();
		try {
		    Job job = Job.getInstance(conf, "CrawledFile Indexer");
		    job.setJarByClass(IndexerHadoopDriver.class);
		    
		    MultithreadedMapper.setMapperClass(job, IndexerHadoopMapper.class);
	        MultithreadedMapper.setNumberOfThreads(job, 10);
		    
//		    job.setMapperClass(IndexerHadoopMapper.class);
	        job.setMapperClass(MultithreadedMapper.class);
	        
	        job.setCombinerClass(IndexerHadoopReducer.class);
	        
		    job.setReducerClass(IndexerHadoopReducer.class);

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
			
			IndexerHadoopDriver cd = new IndexerHadoopDriver();
			cd.indexer(crawlInput, output);
		}
}
