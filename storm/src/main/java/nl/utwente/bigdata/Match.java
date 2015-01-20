package nl.utwente.bigdata;

import java.util.Date;


/**
 * Simple representation of a (planned) match
 * Matches help to determine which country scored according to tweets, as only
 * a few possible countries are left when all matches are known.
 */
public class Match{
	//public Date start;
	public String homeCountry;
	public String awayCountry;
	public String hashtag;
	
	//public String score;
	//public JSONArray homeGoals;
	//public JSONArray awayGoals;
	
	/*@Override
	public int compareTo(Match other) {
		return this.start.compareTo(other.start);
	}*/
	
	@Override
	public String toString() {
		return homeCountry + " - " + awayCountry + "(#" + hashtag + ")";
	}
	
	
}
