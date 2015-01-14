/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.utwente.bigdata.bolts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class CheckGoalBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(CheckGoalBolt.class.getName());
	private transient JSONParser parser;
  
	@SuppressWarnings("rawtypes")
  	@Override
  	public void prepare(Map stormConf, TopologyContext context) {
		parser = new JSONParser();
	}
  
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
	  try 
	  {
        JSONObject tweet = (JSONObject) parser.parse(tuple.getString(0));
        JSONObject entities = (JSONObject) tweet.get("entities");
        JSONArray hashtagArray = (JSONArray) entities.get("hashtags");
        
        String text = (String) tweet.get("text");
        String lang = (String) tweet.get("lang");
        String time = (String) tweet.get("created_at");
        
        List<String> hashtags = new ArrayList<String> ();
        for(int i = 0; i<hashtagArray.size(); i++) 
        {
        	JSONObject hashtag = (JSONObject) hashtagArray.get(i);
        	String hashtagText = (String) hashtag.get("text");
        	
        	if(hashtagText != null) {
        		hashtags.add(hashtagText);
        	}
        }
        
        String country = processTweet(text, lang, hashtags);
        
        if(country != null) {
        	collector.emit(new Values(time, country));
        }
      }
      catch (ClassCastException e) {      	
    	//logger.info(e.toString());
        return;
      }
      catch (org.json.simple.parser.ParseException e) {
    	System.out.println("ParseException");
    	e.printStackTrace();
        return; 
      } 
	  catch (Exception e) {
    	  e.printStackTrace();
      }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	  declarer.declare(new Fields("time", "country"));
  }
  
  private String processTweet(String text, String lang, List<String> hashtags) {
	  
	  if(text == null || lang == null) {
		  return null;
	  }
	  
	  //System.out.println(text.length() + "\t" + lang + "\t" + hashtags);
	  
	  return lang;
  }

}
