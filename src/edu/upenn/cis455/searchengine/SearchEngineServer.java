package edu.upenn.cis455.searchengine;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.deploy.App;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.webapp.WebAppContext;
import org.springframework.core.io.ClassPathResource;

import edu.upenn.cis455.mapreduce.servers.MasterServer;


public class SearchEngineServer {
	
	public static Server server;
	private int port = 8080;

	
	public SearchEngineServer() throws Exception {
		server = new Server(port);
		HandlerCollection handlers = new HandlerCollection();
		WebAppContext webapp = new WebAppContext();	
		webapp.setDescriptor("./conf/search_engine/WEB-INF/web.xml");
		webapp.setResourceBase(new ClassPathResource(".").getURI().toString());
		
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
