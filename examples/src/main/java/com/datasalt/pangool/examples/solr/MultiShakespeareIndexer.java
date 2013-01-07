package com.datasalt.pangool.examples.solr;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.datasalt.pangool.examples.BaseExampleJob;
import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.solr.TupleSolrOutputFormat;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.HadoopOutputFormat;

/**
 * Typical "Shakespeare" example of SOLR indexation but this one makes use of Pangool's facilities to create multiple
 * indexes from the same indexer Job. When given an (uncompressed) shakespeare dump folder like:
 * http://www.it.usyd.edu.au/~matty/Shakespeare/shakespeare.tar.gz it will create four indexes, one per each category
 * (see {@link Category}). It makes use of {@link TupleSolrOutputFormat} for that.
 */
@SuppressWarnings("serial")
public class MultiShakespeareIndexer extends BaseExampleJob implements Serializable {

	public static enum Category {
		COMEDIES, HISTORIES, POETRY, TRAGEDIES
	}

	final static Schema SCHEMA = new Schema("shakespeare",
	    Fields.parse("line:long, text:string, title:string, category:" + Category.class.getName()));
	final static Schema OUT_SCHEMA = new Schema("shakespeare",
	    Fields.parse("line:long, text:string, title:string"));

	/*
	 * Per-category mapper that adds the Category to the intermediate Tuple
	 */
	public static class CategoryMapper extends TupleMapper<LongWritable, Text> {

		Category category;
		String title;
		ITuple tuple = new Tuple(SCHEMA);

		public CategoryMapper(Category category, String title) {
			this.category = category;
			this.title = title;
			tuple.set("title", title);
			tuple.set("category", category);
		}

		@Override
		public void map(LongWritable key, Text value, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException {
			tuple.set("line", key.get());
			tuple.set("text", value.toString());
			collector.write(tuple);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2) {
			failArguments("Invalid number of arguments");
			return -1;
		}
		delete(args[1]);

		TupleMRBuilder job = new TupleMRBuilder(conf);
		job.addIntermediateSchema(SCHEMA);
		job.setGroupByFields("line");

		String input = args[0], output = args[1];
		FileSystem fileSystem = FileSystem.get(conf);

		for(Category category : Category.values()) { // For each Category
			String categoryString = category.toString().toLowerCase();
			// Add the category, book title input spec with the associated CategoryMapper
			for(FileStatus fileStatus : fileSystem.listStatus(new Path(input + "/" + categoryString))) {
				job.addInput(fileStatus.getPath(), new HadoopInputFormat(TextInputFormat.class),
				    new CategoryMapper(category, fileStatus.getPath().getName()));
			}
			// Add a named output for each category
			job.addNamedOutput(categoryString, new TupleSolrOutputFormat(new File(
			    "src/test/resources/shakespeare-solr"), conf), ITuple.class, NullWritable.class);
		}
		job.setOutput(new Path(output), new HadoopOutputFormat(NullOutputFormat.class), ITuple.class,
		    NullWritable.class);
		// The reducer will just emit the tuple to the corresponding Category output
		job.setTupleReducer(new TupleReducer<ITuple, NullWritable>() {

			ITuple outTuple = new Tuple(OUT_SCHEMA);

			public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context,
			    Collector collector) throws IOException, InterruptedException, TupleMRException {

				for(ITuple tuple : tuples) {
					Category category = (Category) tuple.get("category");
					outTuple.set("line", tuple.get("line"));
					outTuple.set("text", tuple.get("text"));
					outTuple.set("title", tuple.get("title"));
					collector.getNamedOutput(category.toString().toLowerCase())
					    .write(outTuple, NullWritable.get());
				}
			}
		});

		try {
			Job hadoopJob = job.createJob();
			hadoopJob.waitForCompletion(true);
		} finally {
			job.cleanUpInstanceFiles();
		}
		return 0;
	}

	public MultiShakespeareIndexer() {
		super("Usage: [input-shakespeare] [output-indexes]");
	}

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new MultiShakespeareIndexer(), args);
	}
}
