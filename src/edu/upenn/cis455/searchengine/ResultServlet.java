package edu.upenn.cis455.searchengine;

import java.io.BufferedReader;
import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

@SuppressWarnings("serial")
public class ResultServlet extends HttpServlet {
	
	public static Logger log = Logger.getLogger(ResultServlet.class);
	
	@Override
	public void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
    		IOException{
		
	}
	
}
