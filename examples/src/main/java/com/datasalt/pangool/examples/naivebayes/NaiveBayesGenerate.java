package com.datasalt.pangool.examples.naivebayes;

import java.io.IOException;
import java.io.Serializable;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.datasalt.pangool.examples.BaseExampleJob;
import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;

/**
 * This class is a simple distributed M/R model generator for performing Naive Bayes text classification tasks. We see
 * how easy it is to perform an efficient M/R job that uses compund registers (3 fields), grouping by two fields and
 * using enumerations ({@link Category}) natively. We also see there is no need for a lot of boilerplate code as we can
 * use instances for everything: Reducers, Mappers, ...
 * <p>
 * The output model can later be read by {@link NaiveBayesClassifier}.
 */
@SuppressWarnings({ "rawtypes", "serial" })
public class NaiveBayesGenerate extends BaseExampleJob implements Serializable {

	// These categories describe a simple sentiment analysis task: a text is either "POSITIVE" or "NEGATIVE"
	public enum Category {
		POSITIVE, NEGATIVE
	}

	private static Schema INTERMEDIATE_SCHEMA = new Schema("categoryCounter", Fields.parse("category:"
	    + Category.class.getName() + ", word:string, count:int"));

	public static String normalizeWord(String word) {
		return word.replaceAll("\\p{Punct}", "").toLowerCase();
	}

	public NaiveBayesGenerate() {
		super("Usage: NaiveBayesGenerate [input_examples] [output_path]");
	}

	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2) {
			failArguments("Wrong number of arguments");
			return -1;
		}
		String inputExamples = args[0];
		String output = args[1];
		delete(output);

		TupleMRBuilder job = new TupleMRBuilder(conf, "Naive Bayes Model Generator");
		job.addIntermediateSchema(INTERMEDIATE_SCHEMA);
		// perform per-category word count mapping
		job.addInput(new Path(inputExamples), new HadoopInputFormat(TextInputFormat.class),
		    new TupleMapper<LongWritable, Text>() {

			    ITuple tuple = new Tuple(INTERMEDIATE_SCHEMA);

			    @Override
			    public void map(LongWritable toIgnore, Text value, TupleMRContext context, Collector collector)
			        throws IOException, InterruptedException {

				    Category category = Category.valueOf(value.toString().split("\t")[0]);
				    StringTokenizer itr = new StringTokenizer(value.toString().split("\t")[1]);
				    tuple.set("category", category);
				    tuple.set("count", 1);
				    while(itr.hasMoreTokens()) {
					    tuple.set("word", normalizeWord(itr.nextToken()));
					    collector.write(tuple);
				    }
			    }
		    });

		TupleReducer countReducer = new TupleReducer<ITuple, NullWritable>() {

			public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context,
			    Collector collector) throws IOException, InterruptedException, TupleMRException {
				int count = 0;
				ITuple outputTuple = null;
				for(ITuple tuple : tuples) {
					count += (Integer) tuple.get("count");
					outputTuple = tuple;
				}
				outputTuple.set("count", count);
				collector.write(outputTuple, NullWritable.get());
			}
		};
		job.setTupleCombiner(countReducer);
		job.setTupleReducer(countReducer);
		job.setGroupByFields("word", "category");
		job.setTupleOutput(new Path(output), INTERMEDIATE_SCHEMA);
		try {
			if(job.createJob().waitForCompletion(true)) {
				return 1;
			}
		} finally {
			job.cleanUpInstanceFiles();
		}
		return -1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new NaiveBayesGenerate(), args);
	}
}