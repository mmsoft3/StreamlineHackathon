package eu.streamline.hackathon.flink.job;

import eu.streamline.hackathon.common.data.GDELTEvent;

public class Aggregate {
	double goldsteinSum;
	double goldsteinAvg;
	int numMentions;
	int numSources;
	int numArticles;
	double avgToneSum;
	double avgToneAvg;
	
	private int count;

	public Aggregate() {
		goldsteinSum = 0.0;
		goldsteinAvg = 0.0;
		numMentions = 0;
		numSources = 0;
		numArticles = 0;
		avgToneSum = 0.0;
		avgToneAvg = 0.0;

		count = 0;
	}
	
	public void calcAggregates(GDELTEvent evt) {
		count++;

		goldsteinSum += evt.goldstein;
		goldsteinAvg = goldsteinSum / count;
		
		numMentions += evt.numMentions;
		numSources += evt.numSources;
		numArticles += evt.numArticles;
		
		avgToneSum += evt.avgTone;
		avgToneAvg = avgToneSum / count;
		
	}
	
	@Override
	public String toString() {
		String out = "";
		out += "goldstein=" + goldsteinSum + "/" + goldsteinAvg;
		out += " numMentions=" + numMentions;
		out += " numSources=" + numSources;
		out += " numArticles=" + numArticles;
		out += " avgTone=" + avgToneSum + "/" + avgToneAvg;
		
		return out;
	}
}
