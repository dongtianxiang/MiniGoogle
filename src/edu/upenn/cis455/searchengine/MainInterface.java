package edu.upenn.cis455.searchengine;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@SuppressWarnings("serial")
public class MainInterface extends HttpServlet{
		
	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
            IOException {
		File f = new File("./html/HomePage.html");
		StringBuilder sb = new StringBuilder();
		Scanner sc = new Scanner(f);
		while (sc.hasNext()){
			sb.append(sc.nextLine() + "\n");
		}
		sc.close();
		resp.setStatus(HttpServletResponse.SC_OK);
		resp.setContentType("text/html");
        PrintWriter pw = resp.getWriter();
        pw.println(sb.toString());
        pw.flush();
	}	
}
