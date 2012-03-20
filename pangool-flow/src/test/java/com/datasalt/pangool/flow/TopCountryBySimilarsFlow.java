package com.datasalt.pangool.flow;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;

import com.datasalt.pangool.flow.io.TextInput;
import com.datasalt.pangool.flow.io.TextOutput;
import com.datasalt.pangool.flow.io.TextSer;
import com.datasalt.pangool.flow.io.TupleInput;
import com.datasalt.pangool.flow.io.TupleOutput;
import com.datasalt.pangool.flow.mapred.ParseMapper;
import com.datasalt.pangool.flow.mapred.SingleSchemaReducer;
import com.datasalt.pangool.flow.mapred.StraightTopReducer;
import com.datasalt.pangool.flow.ops.TupleParser;
import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRException;

/**
 * This is a somewhat more serious flow that executes 3 M/R steps in order to find the top country per each user given a
 * list of similarities between users and a user-country map. It shows the usage of {@link PangoolMRJob}.
 */
public class TopCountryBySimilarsFlow extends LinearFlow {

	/**
	 * Reads a list of pairwise similarities and emits the top N
	 */
	@SuppressWarnings("serial")
	public static class TopSimilarities extends PangoolMRJob {

		int n;

		public TopSimilarities() throws TupleMRException {
			super("topSimilarities", new Inputs("similarityInput"), new Params(new Param("n", Integer.class)),
			    NamedOutputs.NONE, new GroupBy("first"), new OrderBy().add("first", Order.ASC).add("score", Order.DESC));
		}

		@Override
		public void configure(Map<String, Object> parsedParameters) throws TupleMRException {
			// Read the "n" parameter. This is the size of the top.
			this.n = (Integer) parsedParameters.get("n");
			// Define schemas
			final Schema similaritySchema = new Schema("similarity",
			    Fields.parse("first:string, second:string, score:double"));
			// Define input processors (Mappers)
			addInput("similarityInput", new TextInput(new ParseMapper(new TupleParser(similaritySchema, "\t")),
			    similaritySchema));
			// Define the Reducer
			setReducer(new StraightTopReducer(n));
			setOutput(new TupleOutput(similaritySchema));
		}
	}

	/**
	 * Attaches the country of the user in the right side of the relationship. Emits unresolved users to a named output.
	 */
	@SuppressWarnings("serial")
	public static class AttachCountryInfo extends PangoolMRJob {

		public AttachCountryInfo() {
			super("attachCountry", new Inputs("topSimilarities", "countryInfo"), Params.NONE, new NamedOutputs("unresolved"),
			    new GroupBy("second"));
		}

		@Override
		public void configure(Map<String, Object> parsedParameters) throws TupleMRException {
			// Define the schemas
			final Schema similaritySchema = new Schema("similarity",
			    Fields.parse("first:string, second:string, score:double"));
			final Schema countryInfo = new Schema("countryInfo", Fields.parse("second:string, country:string"));
			final Schema jointSchema = new Schema("jointSchema",
			    Fields.parse("first:string, second:string, score:double, country:string"));

			// Define the input processors (Mappers)
			addInput("topSimilarities", new TupleInput(similaritySchema));
			addInput("countryInfo", new TextInput(new ParseMapper(new TupleParser(countryInfo, "\t")), countryInfo));
			// Define the Reducer
			setReducer(new SingleSchemaReducer<ITuple, NullWritable>(jointSchema) {

				TextSer unresolvedId = new TextSer();

				@Override
				public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
				    throws IOException, InterruptedException, TupleMRException {

					String country = null;
					for(ITuple tuple : tuples) {
						if(tuple.getSchema().getName().equals("countryInfo")) {
							country = tuple.get("country").toString();
						} else if(country != null) {
							Utils.shallowCopy(tuple, this.tuple);
							this.tuple.set("country", country);
							collector.write(this.tuple, NullWritable.get());
						} else { // write users that don't have country
							unresolvedId.set(tuple.get("second").toString());
							collector.getNamedOutput("unresolved").write(unresolvedId, NullWritable.get());
						}
					}
				};
			});

			setOutput(new TupleOutput(jointSchema));
			setOutput("unresolved", new TextOutput());
		}
	}

	/**
	 * Emits the top country for each user
	 */
	@SuppressWarnings("serial")
	public static class TopCountry extends PangoolMRJob {

		public TopCountry() {
			super("topCountry", new Inputs("topSimilaritiesJoinCountryInfo"), Params.NONE, NamedOutputs.NONE, new GroupBy(
			    "first"), new OrderBy().add("first", Order.ASC).add("score", Order.DESC));
		}

		@Override
		public void configure(Map<String, Object> parsedParameters) throws TupleMRException {
			// Define schemas
			final Schema jointSchema = new Schema("jointSchema",
			    Fields.parse("first:string, second:string, score:double, country:string"));
			final Schema outSchema = new Schema("outSchema", Fields.parse("first:string, country:string"));

			// Define input processors (Mappers)
			addInput("topSimilaritiesJoinCountryInfo", new TupleInput(jointSchema));
			// Define Reducer
			setReducer(new SingleSchemaReducer<ITuple, NullWritable>(outSchema) {

				@Override
				public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
				    throws IOException, InterruptedException, TupleMRException {

					tuple.set("first", group.get("first").toString());
					tuple.set("country", tuples.iterator().next().get("country").toString());
					collector.write(tuple, NullWritable.get());
				}
			});

			setOutput(new TupleOutput(outSchema));
		}
	}

	public TopCountryBySimilarsFlow(String similarityFile, String countryInfoFile, Integer n, String output)
	    throws TupleMRException {
		// Define the Flow
		add(new TopSimilarities());
		add(new AttachCountryInfo());
		add(new TopCountry());

		add(similarityFile);
		add(countryInfoFile);

		bind("topSimilarities.n", n);
		bind("topSimilarities.similarityInput", similarityFile);

		bind("attachCountry.topSimilarities", "topSimilarities.output");
		bind("attachCountry.countryInfo", countryInfoFile);

		bind("topCountry.topSimilaritiesJoinCountryInfo", "attachCountry.output");

		bind("topCountry.output", output);
	}
}
