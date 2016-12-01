package edu.upenn.cis455.mapreduce.servers;

public class WorkerInformation {
	
	public String status;
	public String job;
	public String keysWritten;
	public String keysRead;
	public String currentJob;
	public String IPAddress;
	public Long lastCheckIn = new Long(0);
}
