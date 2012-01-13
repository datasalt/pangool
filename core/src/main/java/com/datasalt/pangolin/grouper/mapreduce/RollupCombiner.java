/**
 * Copyright [2011] [Datasalt Systems S.L.]
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
package com.datasalt.pangolin.grouper.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;

import com.datasalt.pangolin.grouper.io.tuple.Tuple;



public class RollupCombiner extends RollupReducer<Tuple,NullWritable>{
	
	@Override
	public void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);

	}
	
	@Override
	public void cleanup(Context context) throws IOException,InterruptedException {
		super.cleanup(context);
	}
	
	
}
