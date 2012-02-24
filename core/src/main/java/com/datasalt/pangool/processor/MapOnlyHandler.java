/**
 * Copyright [2012] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasalt.pangool.processor;

import java.io.Serializable;

import org.apache.hadoop.mapreduce.Mapper;

/**
 * Class to be extended by handlers that interact with {@link MapOnlyJobBuilder} API.
 *
 */
@SuppressWarnings("serial")
public abstract class MapOnlyHandler<I1, I2, O1, O2> extends Mapper<I1, I2, O1, O2> implements Serializable {

	protected void map(I1 key, I2 value, org.apache.hadoop.mapreduce.Mapper<I1,I2,O1,O2>.Context context) throws java.io.IOException ,InterruptedException {
		
	};
	
	protected void setup(org.apache.hadoop.mapreduce.Mapper<I1,I2,O1,O2>.Context context) throws java.io.IOException ,InterruptedException {
		
	};
	
	protected void cleanup(org.apache.hadoop.mapreduce.Mapper<I1,I2,O1,O2>.Context context) throws java.io.IOException ,InterruptedException {
		
	};
}
