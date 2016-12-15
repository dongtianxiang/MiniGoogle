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

public class FileWriterQueue implements Runnable {
	public ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<String>();
	private static FileWriter fileWriter;
	private static Map<String, FileWriterQueue> map = new HashMap<>();
	private static final Logger log = Logger.getLogger(FileWriterQueue.class);
	private static FileWriterQueue singleton;
	private static int count = 0;

	private FileWriterQueue(FileWriter fw) {
		fileWriter = fw;
	}

	/**
	 * 
	 * Only can be used one writer per worker node
	 * @param outfile
	 * @param context
	 * @return
	 */
	@Deprecated
	public static synchronized FileWriterQueue getFileWriterQueue(File outfile, TopologyContext context) {
		if(singleton == null) {
			try {
				outfile.getParentFile().mkdirs();
				fileWriter = new FileWriter(outfile, false);
			}
			catch (IOException e) {
				e.printStackTrace();
				log.fatal("file open failed.");
			}
			singleton = new FileWriterQueue(fileWriter);
			context.addStreamTask(singleton);
		}
		return singleton;
	}
	
	/**
	 * 
	 * Only can be used one writer per worker node
	 * @return
	 */
	@Deprecated
	public static FileWriterQueue getFileWriterQueueLaterCall() {
		return singleton;
	}
	
	public static synchronized FileWriterQueue getFileWriterQueueFromMap(File outfile, TopologyContext context) {
		String key = outfile.getAbsolutePath();
		try {
			key = outfile.getCanonicalPath();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		if(map.containsKey(key)) {
			return map.get(key);
		}
		try {
			outfile.getParentFile().mkdirs();
			fileWriter = new FileWriter(outfile, false);
			
		} catch (IOException e) {
			e.printStackTrace();
			log.fatal("file open failed.");
		}
		
		FileWriterQueue fileWriterQueue = new FileWriterQueue(fileWriter);
		context.addStreamTask(fileWriterQueue);
		map.put(key, fileWriterQueue);
		return fileWriterQueue;
	}

	public synchronized void addQueue(String content) {
		queue.offer(content);
		count++;
	}

	@Override
	public void run() {
		while (true) {
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
