package edu.upenn.cis.stormlite.spouts.LocalDBBuilder;

public class LinksSpout extends LinksFileSpout {

	@Override
	public String getFilename() {
		return "internet.txt";
	}
}
