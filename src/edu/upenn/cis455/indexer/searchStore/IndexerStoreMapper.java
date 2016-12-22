package edu.upenn.cis455.indexer.searchStore;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class IndexerStoreMapper extends Mapper<LongWritable, Text, Text, Text>{
	@Override
    protected void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
		String line = value.toString().trim();
	    String[] split = line.split("\t");
	    String word = split[0];
	    char ch = word.charAt(0);
	    if(isValid(ch, 3) || isValid(ch, 4)) {   // mapper for Worker 0, marked with fault tolerance
		    String content = split[1];
			Text keyText = new Text(word);
			Text valueText = new Text(content);
			context.write(keyText, valueText);
	    }
	}
	
	public int getHashing(char ch){   // return hashing value to given character, converting into 0 - 26
		if(ch < 'a') return 0;
		else return ch - 'a' + 1;
	}
	
	public boolean isValid(char ch, int workerIndex) {   // Hashing should make sure Fault Tolerance 
		Set<Integer> set = new HashSet<>();
		if(workerIndex == 0) {
			for(int i = 0; i <= 3; i++) set.add(i);
		}
		if(workerIndex == 1) {
			for(int i = 4; i <= 9; i++) set.add(i);
		}
		if(workerIndex == 2) {
			for(int i = 10; i <= 15; i++) set.add(i);
		}
		if(workerIndex == 3) {
			for(int i = 16; i <= 20; i++) set.add(i);
		}
		if(workerIndex == 4) {
			for(int i = 21; i <= 26; i++) set.add(i);
		}
		return set.contains(getHashing(ch));
	}
}
