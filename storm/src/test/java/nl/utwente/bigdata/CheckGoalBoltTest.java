package nl.utwente.bigdata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import nl.utwente.bigdata.bolts.CheckGoalBolt;

import org.junit.Test;

public class CheckGoalBoltTest {
	
	@Test
	public void testScorePattern(){
		Pattern pattern = CheckGoalBolt.scorePattern;
		
		Matcher matcher = pattern.matcher("#GERBRA 200-1 bla bla");
		if(matcher.find()){
			assertEquals(matcher.group(), "200-1");
			assertEquals(matcher.group(1), "200");
			assertEquals(matcher.group(2), "1");
		}else{
			fail("Pattern did not work");
		}
		
		matcher = pattern.matcher("#GERBRA 200:1 bla bla");
		if(matcher.find()){
			assertEquals(matcher.group(), "200:1");
			assertEquals(matcher.group(1), "200");
			assertEquals(matcher.group(2), "1");
		}else{
			fail("Pattern did not work");
		}
		
	}
}
