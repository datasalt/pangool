package com.datasalt.pangool.examples.topicalwordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangool.examples.BaseExampleJob;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;
import com.datasalt.pangool.tuplemr.mapred.tuplemr.TupleCombiner;
import com.datasalt.pangool.tuplemr.mapred.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.mapred.tuplemr.TupleReducer;

public class TopicalWordCount extends BaseExampleJob {

	@SuppressWarnings("serial")
	public static class TokenizeMapper extends TupleMapper<LongWritable, Text> {

		protected Tuple tuple;
		protected ObjectMapper mapper;

		public void setup(TupleMRContext context, Collector collector) throws IOException, InterruptedException {
			this.mapper = new ObjectMapper();
			tuple = new Tuple(context.getTupleMRConfig().getIntermediateSchema(0));
		};

		@SuppressWarnings("rawtypes")
		@Override
		public void map(LongWritable key, Text value, TupleMRContext context, Collector collector) throws IOException,
		    InterruptedException {

			// Parse the JSON
			Map document = mapper.readValue(value.toString(), Map.class);
			// Set topic Id
			tuple.set("topic", (Integer) document.get("topicId"));
			// Tokenize the text
			StringTokenizer itr = new StringTokenizer((String) document.get("text"));
			tuple.set("count", 1);
			while(itr.hasMoreTokens()) {
				tuple.set("word", itr.nextToken());
				emitTuple(collector); // We are creating a method for this to allow overriding it from subclasses
			}
		}

		protected void emitTuple(Collector collector) throws IOException, InterruptedException {
			collector.write(tuple);
		}
	}

	@SuppressWarnings("serial")
	public static class CountCombiner extends TupleCombiner {

		@Override
		public void onGroupElements(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException, TupleMRException {
			int count = 0;
			ITuple outputTuple = null;
			for(ITuple tuple : tuples) {
				outputTuple = tuple;
				count += (Integer) tuple.get("count");
			}
			outputTuple.set("count", count);
			collector.write(outputTuple);
		}
	}

	@SuppressWarnings("serial")
	public static class CountReducer extends TupleReducer<ITuple, NullWritable> {

		@Override
		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException, TupleMRException {

			int count = 0;
			ITuple outputTuple = null;
			for(ITuple tuple : tuples) {
				count += (Integer) tuple.get("count");
				outputTuple = tuple;
			}
			outputTuple.set("count", count);
			collector.write(outputTuple, NullWritable.get());
		}
	}

	public TopicalWordCount() {
		super("Usage: TopicalWordCount [input_path] [output_path]");
	}

	static Schema getSchema() {
		List<Field> fields = new ArrayList<Field>();
		// The schema has 3 fields: word, topicId and count
		fields.add(Field.create("word", Type.STRING));
		fields.add(Field.create("topic", Type.INT));
		fields.add(Field.create("count", Type.INT));
		return new Schema("schema", fields);
	}

	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2) {
			failArguments("Wrong number of arguments");
		}

		deleteOuput(args[1]);

		TupleMRBuilder cg = new TupleMRBuilder(conf, "Pangool Topical Word Count");
		cg.addIntermediateSchema(getSchema());
		// We will count each (topicId, word) pair
		// Note that the order in which we defined the fields of the Schema is not relevant here
		cg.setGroupByFields("topic", "word");
		cg.addInput(new Path(args[0]), new HadoopInputFormat(TextInputFormat.class), new TokenizeMapper());
		// We'll use a TupleOutputFormat with the same schema than the intermediate schema
		cg.setTupleOutput(new Path(args[1]), getSchema());
		cg.setTupleReducer(new CountReducer());
		cg.setTupleCombiner(new CountCombiner());

		cg.createJob().waitForCompletion(true);

		return 1;
	}
}
