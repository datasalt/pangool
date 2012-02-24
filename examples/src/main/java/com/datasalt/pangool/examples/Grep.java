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
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.datasalt.pangool.cogroup.TupleMRException;
import com.datasalt.pangool.processor.MapOnlyJobBuilder;
import com.datasalt.pangool.processor.MapOnlyHandler;
import com.datasalt.pangool.utils.HadoopUtils;

/**
 * Example of performing a map-only Job with {@link MapOnlyJobBuilder}. You give a regex to GrepHandler and it will emit the
 * lines that match that regex.
 */
public class Grep {

	public static class GrepHandler extends MapOnlyHandler<LongWritable, Text, Text, NullWritable> {

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

	public Job getJob(Configuration conf, String regex, String input, String output) throws IOException,
	    TupleMRException, URISyntaxException {
		HadoopUtils.deleteIfExists(FileSystem.get(conf), new Path(output));
		MapOnlyJobBuilder b = new MapOnlyJobBuilder(conf);
		b.setHandler(new GrepHandler(regex));
		b.setOutput(new Path(output), TextOutputFormat.class, Text.class, NullWritable.class);
		b.addInput(new Path(input), TextInputFormat.class);
		return b.createJob();
	}

	private static final String HELP = "Usage: [regexp] [input_path] [output_path]";

	public static void main(String args[]) throws TupleMRException, IOException, InterruptedException,
	    ClassNotFoundException, URISyntaxException {
		if(args.length != 3) {
			System.err.println("Wrong number of arguments");
			System.err.println(HELP);
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		new Grep().getJob(conf, args[0], args[1], args[2]).waitForCompletion(true);
	}
}
