package nl.utwente.bigdata.util;


/**
 * Simple representation of a (planned) match Matches help to determine which
 * country scored according to tweets, as only a few possible countries are left
 * when all matches are known.
 */
public class Match {
	// public Date start;
	public String homeCountry = "";
	public String awayCountry = "";
	public String hashtag = "";

	@Override
	public String toString() {
		return homeCountry + " - " + awayCountry + "(#" + hashtag + ")";
	}

}
