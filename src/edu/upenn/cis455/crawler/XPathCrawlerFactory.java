package edu.upenn.cis455.crawler;

/**
 * Crawler Factory to create crawlers.
 * @author cis555
 *
 */
public class XPathCrawlerFactory {
	public XPathCrawler getCrawler() {
		return new XPathCrawler();
	}
}
