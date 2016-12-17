package edu.upenn.cis455.searchengine;

import java.util.*;

import javax.servlet.http.HttpServletResponse;

import java.io.*;

public class WorkerThread implements Runnable{

	private HttpServletResponse resp;
	
    public WorkerThread(HttpServletResponse resp){
        this.resp = resp;
    }

    @Override
    public void run() {
              
    }
}
