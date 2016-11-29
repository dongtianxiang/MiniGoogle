package edu.upenn.cis455.mapreduce.servers;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.webapp.WebAppContext;

public class MasterServer {
	
	public static Server server;
	static String resourceBase = "./resources";
	private int port = 8080;

	public MasterServer() {
		server = new Server(port);
	}
	
	public static void main(String[] args) throws Exception {
		
    	Properties props = new Properties();
    	props.load(new FileInputStream("./resources/log4j.properties"));
    	PropertyConfigurator.configure(props);
		HandlerCollection handlers = new HandlerCollection();
		WebAppContext webapp = new WebAppContext();		
		webapp.setResourceBase(resourceBase);
		webapp.setContextPath("/");
		webapp.setDefaultsDescriptor("./target/master/WEB-INF/web.xml");
		handlers.addHandler(webapp);
		ResourceHandler resourceHandler = new ResourceHandler();		
		resourceHandler.setDirectoriesListed(true);
		resourceHandler.setResourceBase(resourceBase);
		handlers.addHandler(resourceHandler);
		new MasterServer();
		server.setHandler(handlers);
		server.start();	
		server.join();
	}

}
