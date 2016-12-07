package edu.upenn.cis.stormlite.spouts.bdb;

public class LinksSpout extends LinksFileSpout {

	@Override
	public String getFilename() {
		return "internet.txt";
	}
}
