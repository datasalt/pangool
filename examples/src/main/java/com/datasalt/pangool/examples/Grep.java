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
package com.datasalt.pangool.examples;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.datasalt.pangool.tuplemr.MapOnlyJobBuilder;
import com.datasalt.pangool.tuplemr.mapred.MapOnlyMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;

/**
 * Example of performing a map-only Job with {@link MapOnlyJobBuilder}. You give a regex to GrepHandler and it will emit the
 * lines that match that regex.
 */
public class Grep extends BaseExampleJob {

	public static class GrepHandler extends MapOnlyMapper<LongWritable, Text, Text, NullWritable> {

		private static final long serialVersionUID = 1L;
		private Pattern regex;

		public GrepHandler(String regex) {
			this.regex = Pattern.compile(Pattern.quote(regex));
		}

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Matcher matcher = regex.matcher(value.toString());
			if(matcher.find()) {
				context.write(value, NullWritable.get());
			}
		};
	}

	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 3) {
			failArguments("Wrond number of arguments");
			return -1;
		}
		
		String regex = args[0];
		String input = args[1];
		String output = args[2];
		
		deleteOutput(output);
		
		MapOnlyJobBuilder b = new MapOnlyJobBuilder(conf);
		b.setMapper(new GrepHandler(regex));
		b.setOutput(new Path(output), TextOutputFormat.class, Text.class, NullWritable.class);
		b.addInput(new Path(input), new HadoopInputFormat(TextInputFormat.class));
		b.createJob().waitForCompletion(true);
		
		return 0;
	}

	public Grep() {
		super("Usage: [regexp] [input_path] [output_path]");
	}

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new Grep(), args);
	}
}
