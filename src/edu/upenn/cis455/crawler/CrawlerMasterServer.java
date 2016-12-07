package edu.upenn.cis455.crawler;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.webapp.WebAppContext;

public class CrawlerMasterServer {
		
		public static Server server;
		static String resourceBase = "./resources";
		private int port = 8080;

		public CrawlerMasterServer() {
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
			webapp.setDefaultsDescriptor("./conf/master_crawler/WEB-INF/web.xml");
			handlers.addHandler(webapp);
			ResourceHandler resourceHandler = new ResourceHandler();		
			resourceHandler.setDirectoriesListed(true);
			resourceHandler.setResourceBase(resourceBase);
			handlers.addHandler(resourceHandler);
			new CrawlerMasterServer();
			server.setHandler(handlers);
			server.start();	
			server.join();
		}

	}

