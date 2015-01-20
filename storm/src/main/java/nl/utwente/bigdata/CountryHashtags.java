package nl.utwente.bigdata;

import java.util.HashMap;
import java.util.Map;

/**
 * This class contains all 3 letter tags for the countries attending the worldcup
 */
public class CountryHashtags {
	public static Map<String, String> hashtags;
	
	/*
	 * Put all tags in the map
	 */
	static{
		hashtags = new HashMap<String, String>();
		
//		BRA Brazil
//		CRO Croatia
//		MEX Mexico
//		CMR Cameroon
//		ESP Spain
//		NED Netherlands
//		CHI Chile
//		AUS Australia
//		COL Columbia
//		GRE Greece
//		URU Uruguay
//		CRC Costa Rica
//		ENG England
//		ITA Italy
//		CIV Cote D’iviore
//		JPN Japan
//		SUI Switzerland
//		ECU Ecuador
//		FRA France
//		HON Honduras
//		ARG Argentina
//		BIH Bosnia
//		GER Germany
//		POR Portugal
//		IRN Iran 
//		NGA Nigeria
//		GHA Ghana
//		USA USA
//		BEL Belgium
//		ALG Algeria
//		RUS Russia
//		KOR Korea
		
		
//		BRA Brazil
		hashtags.put("Brazil", "BRA");
//		CRO Croatia
		hashtags.put("Croatia", "CRO");
//		MEX Mexico
		hashtags.put("Mexico", "MEX");
//		CMR Cameroon
		hashtags.put("Cameroon", "CMR");
//		ESP Spain
		hashtags.put("Spain", "ESP");
//		NED Netherlands
		hashtags.put("Netherlands", "NED");
//		CHI Chile
		hashtags.put("Chile", "CHI");
//		AUS Australia
		hashtags.put("Australia", "AUS");
//		COL Columbia
		hashtags.put("Columbia", "COL");
		hashtags.put("Colombia", "COL");
//		GRE Greece
		hashtags.put("Greece", "GRE");
//		URU Uruguay
		hashtags.put("Uruguay", "URU");
//		CRC Costa Rica
		hashtags.put("Costa Rica", "CRC");
//		ENG England
		hashtags.put("England", "ENG");
//		ITA Italy
		hashtags.put("Italy", "ITA");
//		CIV Cote D’iviore
		hashtags.put("C\u00c3\u00b4te d'Ivoire", "CIV");
//		JPN Japan
		hashtags.put("Japan", "JPN");
//		SUI Switzerland
		hashtags.put("Switzerland", "SUI");
//		ECU Ecuador
		hashtags.put("Ecuador", "ECU");
//		FRA France
		hashtags.put("France", "FRA");
//		HON Honduras
		hashtags.put("Honduras", "HON");
//		ARG Argentina
		hashtags.put("Argentina", "ARG");
//		BIH Bosnia
		hashtags.put("Bosnia", "BIH");
		hashtags.put("Bosnia and Herzegovina", "BIH");
//		GER Germany
		hashtags.put("Germany", "GER");
//		POR Portugal
		hashtags.put("Portugal", "POR");
//		IRN Iran 
		hashtags.put("Iran", "IRN");
//		NGA Nigeria
		hashtags.put("Nigeria", "NGA");
//		GHA Ghana
		hashtags.put("Ghana", "GHA");
//		USA USA
		hashtags.put("USA", "USA");
//		BEL Belgium
		hashtags.put("Belgium", "BEL");
//		ALG Algeria
		hashtags.put("Algeria", "ALG");
//		RUS Russia
		hashtags.put("Russia", "RUS");
//		KOR Korea
		hashtags.put("Korea", "KOR");
		hashtags.put("Korea Republic", "KOR");
	}

	/**
	 * Get the tag for a given countryname
	 * @param country The country to get the tag of
	 * @return
	 */
	public static String get(String country) {
		return hashtags.get(country);
	}
}
