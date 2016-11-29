package edu.upenn.cis455.mapreduce.jobs;

import java.util.Iterator;
import edu.upenn.cis.stormlite.infrastructure.Context;
import edu.upenn.cis.stormlite.infrastructure.Job;

public class PageRankJob implements Job {

	@Override
	public void map(String outLink, String weight, Context context) {
		context.write(outLink, weight);
	}

	@Override
	public void reduce(String pageUrl, Iterator<String> weightsIter, Context context) {
		Double newRank = new Double(0);
		while (weightsIter.hasNext()) {
			newRank += Double.parseDouble(weightsIter.next());
		}
		context.write(pageUrl, newRank.toString());
	}

}
