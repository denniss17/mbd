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

	@Override
	public int hashCode() {
		int hash = 7;
		for (int i = 0; i < hashtag.length(); i++) {
			hash = hash * 31 + hashtag.charAt(i);
		}
		return hash;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Match other = (Match) obj;
		if (hashtag != other.hashtag || awayCountry != other.awayCountry || homeCountry != other.homeCountry)
			return false;
		return true;
	}
}
