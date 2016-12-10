package edu.upenn.cis.stormlite.bolts.bdb;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.infrastructure.TopologyContext;

public class FileWriterQueue implements Runnable{
	public ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<String>();
	private static FileWriterQueue fileWriterQueue;
	private static FileWriter fileWriter;
	private static final Logger log = Logger.getLogger(FileWriterQueue.class);
	private static int count = 0;
	
	private FileWriterQueue(FileWriter fw) {
		fileWriter = fw;		
	}
	
	public static synchronized FileWriterQueue getFileWriterQueue(File outfile, TopologyContext context) {
		if(fileWriterQueue == null) {
			try {
				fileWriter = new FileWriter(outfile, false);
			}
			catch (IOException e) {
				e.printStackTrace();
				log.fatal("file open failed.");
			}
			fileWriterQueue = new FileWriterQueue(fileWriter);
			context.addStreamTask(fileWriterQueue);
		}
		return fileWriterQueue;
	}
	
	public static FileWriterQueue getFileWriterQueueLaterCall() {
		return fileWriterQueue;
	}
	
	public synchronized void addQueue(String content) {
		queue.offer(content);
		count++;
		System.out.println("CONTENT:"+content);
		System.out.println("count:"+count);
	}

	@Override
	public void run() {
		while(true) {
			String content = queue.poll();
			if (content == null)
				Thread.yield();
			else {
				try {
					fileWriter.write(content);
					fileWriter.flush();
				} catch (IOException e) {
					StringWriter sw = new StringWriter();
					PrintWriter pw = new PrintWriter(sw);
					e.printStackTrace(pw);
					log.error(sw.toString());
				}
				
			}
		}
		
	}
	
	
}
