package edu.upenn.cis455.searchengine;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.webapp.WebAppContext;

public class SearchEngineServer {
	
	public static Logger log = Logger.getLogger(SearchEngineServer.class);
	
	public static Server server;
	private int port = 8080;
	
	public SearchEngineServer() throws Exception {
		server = new Server(port);
		HandlerCollection handlers = new HandlerCollection();
		WebAppContext webapp = new WebAppContext();	
		webapp.setDescriptor("./conf/search_engine/WEB-INF/web.xml");
		webapp.setResourceBase("");
		
		// can not load local resources
		handlers.addHandler(webapp);
		server.setHandler(handlers);
		server.start();	
		server.join();
	}
	
	public static void main(String[] args) throws Exception {		
    	Properties props = new Properties();
    	props.load(new FileInputStream("./resources/log4j.properties"));
    	PropertyConfigurator.configure(props);
		new SearchEngineServer();
	}

}
