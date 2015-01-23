package nl.utwente.bigdata.util;

import java.util.HashMap;
import java.util.Map;

public class ReportedScores {

	private int threshold = 10;
	private Map<Score, Integer> scoreMap;

	public ReportedScores(String h, int t) 
	{
		threshold = t;
		scoreMap = new HashMap<Score, Integer> (); 
	}
	
	public void insertScore(Score s) {

		if (scoreMap.containsKey(s)) 
		{
			Integer scoreCount = scoreMap.get(s);
			scoreMap.put(s, scoreCount + 1);
		} else {
			scoreMap.put(s, 1);
		}
	}

	public Score getMostMentionedScore() {

		Score currentTopScore = new Score(0,0);
		Integer currentTopInt = 0;
		
		for (Map.Entry<Score, Integer> entry : scoreMap.entrySet()) {
			if(entry.getValue() > currentTopInt) {
				currentTopInt = entry.getValue();
				currentTopScore = entry.getKey();
			}
		}

		if(currentTopInt > threshold) 
		{
			return currentTopScore;
		} 
		else 
		{
			return null;
		}
		
	}
}
