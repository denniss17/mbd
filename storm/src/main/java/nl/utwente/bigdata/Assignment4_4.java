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

package nl.utwente.bigdata;

public class Assignment4_4 {   
	/*
	 * This depends on the bolt which receives the output of the TopCounterBolt
	 * 
	 * Fields grouping is usefull when the next bolt combines the results of all TopCounterBolts.
	 * Each TopCounterBolt can now run on a subset of the input, for example by looking to the first 
	 * character of the words.
	 * 
	 * Shuffle grouping however could also give reasonable results, as it is probable that the most frequent values 
	 * of the individual TopCounterBolts would be approximately the same when using only one bolt.
	 * 
	 * All and global grouping would only abolish the benefit of having multiple instances, as this would simply
	 * send all input to all instances or to a single instance respectively.
	 */
	
	
}
