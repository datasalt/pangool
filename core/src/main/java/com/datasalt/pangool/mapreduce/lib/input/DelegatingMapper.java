/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datasalt.pangool.mapreduce.lib.input;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasalt.pangool.api.InputProcessor;
import com.datasalt.pangool.commons.DCUtils;

/**
 * An {@link Mapper} that delegates behavior of paths to multiple other mappers.
 * 
 * @see PangoolMultipleInputs#addInputPath(Job, Path, Class, Class)
 */
public class DelegatingMapper extends Mapper {

	Mapper delegate; // The delegate

	static Logger log = LoggerFactory.getLogger(DelegatingMapper.class);
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		// Find the InputProcessor from the TaggedInputSplit.
		if(delegate == null) {
			TaggedInputSplit inputSplit = (TaggedInputSplit) context.getInputSplit();
			log.info("[profile] Got input split. Going to look at DC.");
			delegate = DCUtils.loadSerializedObjectInDC(context.getConfiguration(), Mapper.class,
			    inputSplit.getInputProcessorFile());
			log.info("[profile] Finished. Calling run() on delegate.");
		}
		delegate.run(context);
	}
}
