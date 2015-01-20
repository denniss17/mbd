package nl.utwente.bigdata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Map;

import nl.utwente.bigdata.util.Match;
import nl.utwente.bigdata.util.WorldCupReader;

import org.junit.Test;

public class WorldCupReaderTest {

	/**
	 * Test if the file reading is successful
	 */
	@Test
	public void testLoadData() {
		try {
			String content = WorldCupReader.getInstance().loadRawData();
			assertNotNull(content);
			assertFalse(content.trim().equals(""));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Test if the matches are correctly loaded
	 */
	@Test
	public void testLoadMatches() {
		Map<String, Match> matches = WorldCupReader.getInstance().getMatches();
		assertNotNull(matches);
		assertFalse(matches.isEmpty());
		// 6 matches in group, 8 groups = 48 matches
		// 16 finals = 8 matches
		// quater finals = 4 matches
		// semi finals = 2 matches
		// finals = 2 matches
		// total = 64
		assertEquals(matches.size(), 64);
	}
	

}
