package nl.utwente.bigdata;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

public class WorldCupReaderTest {

	/**
	 * Test if the file reading is successfull
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
		List<Match> matches = WorldCupReader.getInstance().getMatches();
		assertNotNull(matches);
		assertFalse(matches.isEmpty());
	}

}
