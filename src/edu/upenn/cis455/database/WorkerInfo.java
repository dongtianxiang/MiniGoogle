package edu.upenn.cis455.database;

public class WorkerInfo {
	
	public String status;
	public String job;
	public String keysWritten;
	public String keysRead;
	public String currentJob;
	public String IPAddress;
	public Long lastCheckIn = new Long(0);
}
