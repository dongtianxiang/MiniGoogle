package edu.upenn.cis455.searchengine;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;

public class TestJsoup {
	
	public static void main(String[] args) {
		
//		Logger log = Logger.getLogger(TestJsoup.class);			
        File f = new File("./html/FakeResult.html");
//        int start = 10;   
        Document d;
		try {
			d = Jsoup.parse(f, "utf-8");
	        for (int i = 0; i < 9; i++) {
	        	Node n = d.getElementById("h" + i);
	        	System.out.println(n.attr("href"));
	        }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
