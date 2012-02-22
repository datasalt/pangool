package com.datasalt.pangool.examples.grep;

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

import com.datasalt.pangool.cogroup.CoGrouperException;
import com.datasalt.pangool.processor.Processor;
import com.datasalt.pangool.processor.ProcessorHandler;
import com.datasalt.pangool.utils.HadoopUtils;

/**
 * Example of performing a map-only Job with {@link Processor}. You give a regex to GrepProcessor and it will emit the
 * lines that match that regex.
 * 
 * @author pere
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
			if(matcher.find()) {
				context.write(value, NullWritable.get());
			}
		};
	}

	public Job getJob(Configuration conf, String regex, String input, String output) throws IOException,
	    CoGrouperException, URISyntaxException {
		HadoopUtils.deleteIfExists(FileSystem.get(conf), new Path(output));
		Processor processor = new Processor(conf);
		processor.setHandler(new GrepProcessor(regex));
		processor.setOutput(new Path(output), TextOutputFormat.class, Text.class, NullWritable.class);
		processor.addInput(new Path(input), TextInputFormat.class);
		return processor.createJob();
	}

	private static final String HELP = "Usage: grep [regexp] [input_path] [output_path]";

	public static void main(String args[]) throws CoGrouperException, IOException, InterruptedException,
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
