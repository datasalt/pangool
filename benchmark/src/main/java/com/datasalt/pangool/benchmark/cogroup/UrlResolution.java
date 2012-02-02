package com.datasalt.pangool.benchmark.cogroup;

import java.io.IOException;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.datasalt.pangool.CoGrouper;
import com.datasalt.pangool.CoGrouperConfigBuilder;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Sorting;
import com.datasalt.pangool.SortingBuilder;
import com.datasalt.pangool.api.GroupHandler;
import com.datasalt.pangool.api.InputProcessor;
import com.datasalt.pangool.commons.HadoopUtils;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Tuple;

/**
 * Code for solving the URL Resolution CoGroup Problem in Pangool.
 * <p>
 * The URL Resolution CoGroup Problem is: We have one file with URL Registers: {url timestamp ip} and another file with
 * canonical URL mapping: {url canonicalUrl}. We want to obtain the URL Registers file with the url substituted with the
 * canonical one according to the mapping file: {canonicalUrl timestamp ip}.
 */
public class UrlResolution {

	public final static int SOURCE_URL_MAP = 0;
	public final static int SOURCE_URL_REGISTER = 1;

	@SuppressWarnings("serial")
	public static class UrlProcessor extends InputProcessor<LongWritable, Text> {

		public final static int URL = 0, SOURCEID = 1, TIMESTAMP = 2, IP = 3;
		Tuple tuple = new Tuple(4);

		@Override
		public void process(LongWritable key, Text value, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException {

			String[] fields = value.toString().split("\t");
			tuple.setString(URL, Utf8.getBytesFor(fields[0]));
			tuple.setInt(SOURCEID, SOURCE_URL_REGISTER);
			tuple.setLong(TIMESTAMP, Long.parseLong(fields[1]));
			tuple.setString(IP, Utf8.getBytesFor(fields[2]));
			collector.write(tuple);
		}
	}

	@SuppressWarnings("serial")
	public static class UrlMapProcessor extends InputProcessor<LongWritable, Text> {

		public final static int URL = 0, SOURCEID = 1, CANNONICAL_URL = 2;
		Tuple tuple = new Tuple(3);

		@Override
		public void process(LongWritable key, Text value, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException {

			String[] fields = value.toString().split("\t");
			tuple.setString(URL, Utf8.getBytesFor(fields[0]));
			tuple.setInt(SOURCEID, SOURCE_URL_MAP);
			tuple.setString(CANNONICAL_URL, Utf8.getBytesFor(fields[1]));
			collector.write(tuple);
		}
	}

	@SuppressWarnings("serial")
	public static class Handler extends GroupHandler<Text, NullWritable> {

		Text result;

		@Override
		public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException, CoGrouperException {

			if(result == null) {
				result = new Text();
			}
			String cannonicalUrl = null;
			for(ITuple tuple : tuples) {
				if(tuple.getInt(1) == SOURCE_URL_MAP) {
					cannonicalUrl = new Utf8(tuple.getString(2)).toString();
				} else { // SOURCE_URL_REGISTER
					result.set(cannonicalUrl + "\t" + tuple.getLong(UrlProcessor.TIMESTAMP) + "\t"
					    + new Utf8(tuple.getString(UrlProcessor.IP)));
					collector.write(result, NullWritable.get());
				}
			}
		}
	}

	public Job getJob(Configuration conf, String input1, String input2, String output) throws CoGrouperException,
	    IOException {
		// Configure schema, sort and group by
		Schema schema1 = Schema.parse("url:string, timestamp:long, ip:string");
		Schema schema2 = Schema.parse("url:string, cannonicalUrl:string");
		Sorting sort = new SortingBuilder().add("url").addSourceId().buildSorting();
		CoGrouperConfigBuilder config = new CoGrouperConfigBuilder();
		config.addSchema(SOURCE_URL_REGISTER, schema1);
		config.addSchema(SOURCE_URL_MAP, schema2);
		config.setGroupByFields("url");
		config.setSorting(sort).build();

		CoGrouper grouper = new CoGrouper(config.build(), conf);
		// Input / output and such
		grouper.setGroupHandler(new Handler());
		grouper.setOutput(new Path(output), TextOutputFormat.class, Text.class, NullWritable.class);
		grouper.addInput(new Path(input1), TextInputFormat.class, new UrlMapProcessor());
		grouper.addInput(new Path(input2), TextInputFormat.class, new UrlProcessor());
		return grouper.createJob();
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException,
	    CoGrouperException {

		Configuration conf = new Configuration();
		FileSystem fS = FileSystem.get(conf);
		String input1 = args[0];
		String input2 = args[1];
		String output = args[2];
		HadoopUtils.deleteIfExists(fS, new Path(output));
		new UrlResolution().getJob(conf, input1, input2, output).waitForCompletion(true);
	}
}
