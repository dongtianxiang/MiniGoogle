package edu.upenn.cis.stormlite.spouts;

public class WordFileSpout extends SimpleFileSpout {
	@Override
	public String getFilename() {
		return "words.txt";
	}
}
