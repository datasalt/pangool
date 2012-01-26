package com.datasalt.pangool.grep;

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

import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.commons.HadoopUtils;
import com.datasalt.avrool.processor.Processor;
import com.datasalt.avrool.processor.ProcessorHandler;

/**
 * Example of performing a map-only Job with {@link Processor}. You give a regex to GrepProcessor and it will emit
 * the lines that match that regex.
 * 
 * 
 *
 */
public class Grep {

	public static class GrepProcessor extends ProcessorHandler<LongWritable, Text, Text, NullWritable> {

    private static final long serialVersionUID = 1L;
		private Pattern regex;
		
		public GrepProcessor(String regex) {
			this.regex = Pattern.compile(Pattern.quote(regex));
		}
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Matcher matcher = regex.matcher(value.toString());
			if(matcher.matches()) {
				context.write(value, NullWritable.get());
			}			
		};
	}
	
	public Job getJob(Configuration conf, String regex, String input, String output) throws IOException, CoGrouperException, URISyntaxException {
		HadoopUtils.deleteIfExists(FileSystem.get(conf), new Path(output));
		Processor processor = new Processor(conf);
		processor.setHandler(new GrepProcessor(regex));
		processor.setOutput(new Path(output), TextOutputFormat.class, Text.class, NullWritable.class);
		processor.addInput(new Path(input), TextInputFormat.class);
		return processor.createJob();
	}
}
