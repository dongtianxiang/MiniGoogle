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
				
        File f = new File("./html/FakeResult.html");  
        Document d;
        String description = "";
        int dSize = 0, DLIMIT = 100;
        String[] queryList = {"", ""};
		try {
			d = Jsoup.parse(f, "utf-8");
			if (dSize < DLIMIT) {
				Element el = d.select("head").first();
				Elements em3 = el.children();
				for (Element e: em3) {
					if (dSize > DLIMIT) {
						description = description.substring(0, DLIMIT);
						description += "...";
						dSize += 3;
						break;
					}
					String ownText = e.ownText();
					String tag = e.tagName();
					for (String s: queryList) {
						if (ownText.toLowerCase().contains(s.toLowerCase())) {
							ownText = ownText.replace(s, "<p><b>" + s + "</b></p>");
						}
					}
				description += " " + ownText;
				dSize += ownText.length();
				}
			}
	        
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
