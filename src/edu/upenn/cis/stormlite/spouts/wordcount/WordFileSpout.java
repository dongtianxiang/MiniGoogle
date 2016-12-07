package edu.upenn.cis.stormlite.spouts.wordcount;

public class WordFileSpout extends SimpleFileSpout {
	@Override
	public String getFilename() {
		return "words.txt";
	}
}
