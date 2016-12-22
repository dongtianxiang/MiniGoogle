package edu.upenn.cis455.indexer.searchStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.log4j.Logger;

import edu.upenn.cis455.SearchWorkerServer.*;
import edu.upenn.cis455.indexer.hadoop.IndexerHadoopReducer;

public class IndexerStoreReducer extends Reducer<Text, Text, Text, Text>{
	private static final Logger log = Logger.getLogger(IndexerStoreReducer.class);
	private static SearchDBWrapper db = SearchDBWrapper.getInstance("/Users/dongtianxiang/git/ldyd-cis555project/DistributedDB/indexer4");
	
	protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
		Iterator<Text> iter = values.iterator();
		int count = 0;
		Word word = db.getWord(key.toString());
		if(word == null) word = new Word(key.toString());
		while (iter.hasNext()) {
			count++;
			String line = iter.next().toString();
			String[] split = line.split(" ");
			String url = split[0];
			String tf = split[1];
			boolean title = false;
			if(tf.endsWith(":T")) {
				title = true;
				tf = tf.substring(0, tf.length() - 2);
			}
			Double tfvalue = Double.parseDouble(tf);
			word.putTF(url, tfvalue);
			if(title) word.putTitle(url);
		}
		Double idfvalue = Math.log10(242938 * 1.0 / count);
		word.setIdf(idfvalue);
		for(String url : word.getTf().keySet()) {
			Double tf = word.getTF(url);
			Double weight = tf * idfvalue;      // TF * IDF gets the Indexer Value
			word.putWeight(url, weight);
		}
		db.putWord(word);
    }
	
	public static void main(String[] args) {
		log.info("Size --> " + db.wordListSize());
		long time1 = System.currentTimeMillis();
		List<String> list = db.getWordList();
//		Word word = db.getWord("apple");
		for(String word : list) {
//			if(word.getTitle().containsKey(url)) {
//				log.info("word" + " --- >" + url + " Indexer Weight: " + word.getWeight(url));
//			}
			log.info(word);
		}
		System.out.println(list.size());
		long time2 = System.currentTimeMillis();
//		System.out.println(time2 - time1 + "ms");
	}
}
