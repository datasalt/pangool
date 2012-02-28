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
package com.datasalt.pangool.tuplemr.mapred.lib.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.datasalt.pangool.utils.DCUtils;

/**
 * An {@link InputFormat} that delegates behavior of paths to multiple other InputFormats.
 * <p>
 * 
 * @see PangoolMultipleInputs#addInputPath(Job, Path, Class, Class)
 */
@SuppressWarnings("rawtypes")
public class DelegatingInputFormat<K, V> extends InputFormat<K, V> {

	@SuppressWarnings("unchecked")
	public List<InputSplit> getSplits(JobContext job) throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
		Job jobCopy = new Job(conf);
		List<InputSplit> splits = new ArrayList<InputSplit>();

		Map<Path, String> formatMap = PangoolMultipleInputs.getInputFormatMap(job);
		Map<Path, String> mapperMap = PangoolMultipleInputs.getInputProcessorFileMap(job);

		for(Map.Entry<Path, String> entry : formatMap.entrySet()) {
			FileInputFormat.setInputPaths(jobCopy, entry.getKey());
			InputFormat inputFormat = DCUtils.loadSerializedObjectInDC(conf, InputFormat.class, entry.getValue(), true);
			List<InputSplit> pathSplits = inputFormat.getSplits(jobCopy);
			for(InputSplit pathSplit : pathSplits) {
				splits.add(new TaggedInputSplit(pathSplit, conf, entry.getValue(), mapperMap.get(entry.getKey())));
			}
		}

		return splits;
	}

	@Override
	public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,
	    InterruptedException {
		return new DelegatingRecordReader<K, V>(split, context);
	}
}
