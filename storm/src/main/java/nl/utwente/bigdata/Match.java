package nl.utwente.bigdata;


/**
 * Simple representation of a (planned) match
 * Matches help to determine which country scored according to tweets, as only
 * a few possible countries are left when all matches are known.
 */
public class Match implements Comparable<Match>{
	public String start;
	public String homeCountry;
	public String awayCountry;
	
	//public String score;
	//public JSONArray homeGoals;
	//public JSONArray awayGoals;
	
	@Override
	public int compareTo(Match other) {
		return this.start.compareTo(other.start);
	}
	
	@Override
	public String toString() {
		return start + " " + homeCountry + " - " + awayCountry;
	}
	
	
}
