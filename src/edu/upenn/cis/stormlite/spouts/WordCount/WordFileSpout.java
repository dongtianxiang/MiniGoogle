package edu.upenn.cis.stormlite.spouts.WordCount;

public class WordFileSpout extends SimpleFileSpout {
	@Override
	public String getFilename() {
		return "words.txt";
	}
}
