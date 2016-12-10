package edu.upenn.cis455.searchengine;

import java.util.*;
import java.io.*;

public class WorkerThread implements Runnable{

	private String word;
	private Hashtable<String, String> fakedb;
	
    public WorkerThread(String s){
        this.word=s;
        fakedb = new Hashtable<>();
        File f = new File("./fakedb.txt");
        synchronized (f) { 
        	try {
				Scanner sc = new Scanner(f);
				while (sc.hasNext()) {
					String record = sc.nextLine();
				}
			} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
			}
        }

    }

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName()+" Word = "+word);

       
    }
}
