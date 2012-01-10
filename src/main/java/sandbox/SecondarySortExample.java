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
 * Playing with epalace's cool Grouper :-)
 * 
 * @author pere
 *
 */
public class SecondarySortExample {

	/**
	 * 
	 * @author pere
	 *
	 */
	private static class MyInputProcessor extends InputProcessor<LongWritable, Text> {

		private Tuple outputTuple;
		
		@Override
    public void setup(Schema schema, Context context) throws IOException,
        InterruptedException {
	  
			/*
			 * Could be blank by default or have some default logic
			 */
			this.outputTuple = TupleFactory.createTuple(schema);
    }

		@Override
    public void process(LongWritable key, Text value, com.datasalt.pangolin.grouper.mapreduce.InputProcessor.Collector collector)
        throws IOException, InterruptedException, GrouperException {
	    
			String[] tokens = value.toString().split("\\s+");
			String url = tokens[0];
			Long fetched = Long.parseLong(tokens[1]);
			String content = tokens[2];
			
			try {
				outputTuple.setString("url", url);
				outputTuple.setLong("fetched", fetched);
				outputTuple.setString("content", content);
				collector.write(outputTuple);
			} catch(InvalidFieldException e) {
      	/*
      	 * I don't know if it's worth having this Exception be catched
      	 */
				throw new RuntimeException(e);
			}	    
    }
	}
	
	/**
	 * 
	 * @author pere
	 *
	 */
	private static class MyGroupHandler extends GroupHandler<Text,Text> {

		@Override
    public void onOpenGroup(int depth, String field, ITuple firstElement, Context context) throws IOException,
        InterruptedException {
	    
			/*
			 * These could be blank by default
			 */
    }

		@Override
    public void onCloseGroup(int depth, String field, ITuple lastElement, Context context) throws IOException,
        InterruptedException {
	    
			/*
			 * These could be blank by default
			 */
    }
		
		Text cachedContent = new Text();
		Text cachedUrl     = new Text();

		@Override
    public void onGroupElements(Iterable<ITuple> tuples, Context context) throws IOException, InterruptedException {
	    ITuple firstFetchedVersion = tuples.iterator().next();
	    try {
		    cachedUrl.set(firstFetchedVersion.getString("url"));
		    cachedContent.set(firstFetchedVersion.getString("content"));
	      context.write(cachedUrl, cachedContent);
      } catch(InvalidFieldException e) {
      	/*
      	 * I don't know if it's worth having this Exception be catched
      	 */
	      throw new RuntimeException(e);
      }
    }
	}
	
	public static void main(String[] args) throws IOException, GrouperException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		
		/*
		 * We have a dataset of URLs that have been fetched multiple times.
		 */
		Schema schema = Schema.parse("url:string, fetched:long, content:string");

		/*
		 * The input file is a tabulated file
		 */
		Files.write( "url1\t10\tcontent11"
				+ "\n" + "url2\t20\tcontent21"
				+ "\n" + "url2\t21\tcontent21"
				+ "\n" + "url1\t11\tcontent12", new File("input-sandbox-pere"), Charset.forName("UTF-8"));

		/*
		 * We want to sort the URLs by fetched ASC because we want to receive the first
		 * fetched version first.
		 */
		SortCriteria sortCriteria = SortCriteria.parse("url ASC, fetched ASC");

		/*
		 * Mandatory things
		 */
		Grouper grouper = new Grouper(conf);
		grouper.setSchema(schema);

		grouper.addInput(new Path("input-sandbox-pere"), TextInputFormat.class, MyInputProcessor.class);

		/*
		 * (Potentially) optional things
		 */
		grouper.setSortCriteria(sortCriteria);
		grouper.setOutputFormat(TextOutputFormat.class); // could be one by default		
		grouper.setOutputHandler(MyGroupHandler.class); // could be Identity Handler?
		grouper.setFieldsToGroupBy("url"); // could be the first in the schema or something?
		grouper.setOutputKeyClass(Text.class);
		grouper.setOutputValueClass(Text.class);
		
		Path output = new Path("output-sandbox-pere");
		grouper.setOutputPath(output);
		
		Job job = grouper.createJob();
		HadoopUtils.deleteIfExists(FileSystem.get(conf), output);
		
		job.waitForCompletion(true);
		
		for(String line: Files.readLines(new File("output-sandbox-pere/part-r-00000"), Charset.forName("UTF-8"))) {
			System.out.println(line);
		}
	}
}