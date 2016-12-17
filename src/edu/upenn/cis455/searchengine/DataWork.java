package edu.upenn.cis455.searchengine;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Random;

public class DataWork implements Runnable {
	
	private String word;
	private String inputFile;
	private int index;
	private FileWriter fw;
	
	public DataWork(String word, String inputFile, int index) {
		this.word = word;
		this.inputFile = inputFile;
		this.index = index;
		try {
			fw = new FileWriter(inputFile, true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	@Override
	public void run() {	
		synchronized (this) {
			try {
				if (index == 1) {
					fw.write("apple->doc1:0.5->[apple,company]\n");
					fw.write("apple->doc2:0.3->[apple,company]\n");
					fw.write("apple->doc3:0.42->[apple,company]\n");
					fw.flush();
					fw.close();
				} else {				
					fw.write("company->doc2:0.25->[apple,company]\n");
					fw.write("company->doc3:0.2->[apple,company]\n");
					fw.flush();
					fw.close();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
