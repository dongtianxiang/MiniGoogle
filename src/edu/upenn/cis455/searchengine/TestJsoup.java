package edu.upenn.cis455.searchengine;

import java.io.File;
import java.io.IOException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class TestJsoup {
	
	public static void main(String[] args) {
		
        File f = new File("./testpage/www.apple.com.about..html");
        Document d;
		try {
			d = Jsoup.parse(f, "utf-8");
			if (d.select("meta[name~=\"*description*\"]") == null) {
				System.out.println("null");
			} else {
				System.out.println("no");
			}
//			System.out.println(d.select("meta[name~=\"*description*\"]"));
//			resultPage.getElementById("placeholder").text("apple");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        		
	}
}
