package sandbox;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.datasalt.pangolin.commons.HadoopUtils;
import com.datasalt.pangolin.grouper.Grouper;
import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.Schema;
import com.datasalt.pangolin.grouper.SortCriteria;
import com.datasalt.pangolin.grouper.io.tuple.ITuple;
import com.datasalt.pangolin.grouper.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangolin.grouper.io.tuple.Tuple;
import com.datasalt.pangolin.grouper.io.tuple.TupleFactory;
import com.datasalt.pangolin.grouper.mapreduce.InputProcessor;
import com.datasalt.pangolin.grouper.mapreduce.handler.GroupHandler;
import com.google.common.io.Files;

/**
 * Example of getting the first and last element from a group using rollup
 * 
 * @author pere
 * 
 */
public class FirstAndLastExample {

	/**
	 * 
	 * @author pere
	 * 
	 */
	private static class MyInputProcessor extends InputProcessor<LongWritable, Text> {

		private Tuple outputTuple;

		@Override
		public void setup(Schema schema, org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException,
		    InterruptedException, GrouperException {
			outputTuple = TupleFactory.createTuple(context.getConfiguration());
		}

		@Override
		public void process(LongWritable key, Text value,
		    com.datasalt.pangolin.grouper.mapreduce.InputProcessor.Collector collector) throws IOException,
		    InterruptedException, GrouperException {
			String[] tokens = value.toString().split("\\s+");
			String url = tokens[0];
			Long fetched = Long.parseLong(tokens[1]);
			String content = tokens[2];

			outputTuple.setString("url", url);
			outputTuple.setLong("fetched", fetched);
			outputTuple.setString("content", content);
			collector.write(outputTuple);
		}
	}

	/**
	 * 
	 * @author pere
	 * 
	 */
	private static class MyGroupHandler extends GroupHandler<Text, Text> {

		Text firstContent = new Text();
		Text content = new Text();
		Text cachedUrl = new Text();

		@Override
		public void onOpenGroup(int depth, String field, ITuple firstElement, Context context) throws IOException,
		    InterruptedException {

			try {
				firstContent.set(firstElement.getString("content"));
			} catch(InvalidFieldException e) {
				throw new RuntimeException(e);
			}

		}

		@Override
		public void onCloseGroup(int depth, String field, ITuple lastElement, Context context) throws IOException,
		    InterruptedException {

			try {
				cachedUrl.set(lastElement.getString("url"));
				content.set("First content: [" + firstContent + "] Last content: [" + lastElement.getString("content") + "]");
				context.write(cachedUrl, content);
			} catch(InvalidFieldException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void onGroupElements(Iterable<ITuple> tuples, Context context) throws IOException, InterruptedException {
		}
	}

	public static void main(String[] args) throws IOException, GrouperException, InterruptedException,
	    ClassNotFoundException {
		Configuration conf = new Configuration();

		/*
		 * We have a dataset of URLs that have been fetched multiple times.
		 */
		Schema schema = Schema.parse("url:string, fetched:long, content:string");

		/*
		 * The input file is a tabulated file
		 */
		Files.write("url1\t10\tcontent11" + "\n" + "url1\t11\tcontent12" + "\n" + "url1\t12\tcontent13" + "\n"
		    + "url2\t21\tcontent21" + "\n" + "url2\t22\tcontent22" + "\n" + "url2\t23\tcontent23", new File(
		    "input-minmax-pere"), Charset.forName("UTF-8"));

		SortCriteria sortCriteria = SortCriteria.parse("url ASC, fetched ASC");

		Grouper grouper = new Grouper(conf);
		grouper.setSchema(schema);

		grouper.addInput(new Path("input-minmax-pere"), TextInputFormat.class, MyInputProcessor.class);

		grouper.setSortCriteria(sortCriteria);
		grouper.setOutputFormat(TextOutputFormat.class);
		grouper.setOutputHandler(MyGroupHandler.class);
		grouper.setFieldsToGroupBy("url");
		/*
		 * 
		 */
		grouper.setRollupBaseFieldsToGroupBy("url");

		grouper.setOutputKeyClass(Text.class);
		grouper.setOutputValueClass(Text.class);

		Path output = new Path("output-minmax-pere");
		grouper.setOutputPath(output);
		
		Job job = grouper.createJob();
		HadoopUtils.deleteIfExists(FileSystem.get(conf), output);

		job.waitForCompletion(true);

		System.out.println(Files.toString(new File("output-minmax-pere/part-r-00000"), Charset.forName("UTF-8")));
	}
}