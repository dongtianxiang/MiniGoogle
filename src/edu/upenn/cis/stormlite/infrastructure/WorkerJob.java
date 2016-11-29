package edu.upenn.cis.stormlite.infrastructure;

import java.io.Serializable;

/**
 * Simple object to pass along topology and
 * config info
 * 
 * @author zives
 *
 */
public class WorkerJob implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Topology topology;
	Configuration config;
	
	public WorkerJob() {
		
	}
	
	public WorkerJob(Topology topology, Configuration config) {
		super();
		this.topology = topology;
		this.config = config;
	}

	public Topology getTopology() {
		return topology;
	}

	public void setTopology(Topology topology) {
		this.topology = topology;
	}

	public Configuration getConfig() {
		return config;
	}

	public void setConfig(Configuration config) {
		this.config = config;
	}

}
