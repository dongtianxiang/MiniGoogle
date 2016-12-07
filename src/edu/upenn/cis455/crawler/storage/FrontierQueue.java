package edu.upenn.cis455.crawler.storage;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class FrontierQueue {
	@PrimaryKey
	private String key;
	private Queue<String> queue;
	
	public FrontierQueue() {
		key = "FrontierQueue";
		queue = new LinkedList<String>();
	}
	
	public void addQueue(String url) {
		this.queue.offer(url);
	}
	
	public String pollQueue() {
		if(queue.isEmpty()) return null;
		return this.queue.poll();
	}
	
	public int getSize(){
		return this.queue.size();
	}
	
	public boolean isEmpty(){
		return this.queue.size() == 0;
	}
}
