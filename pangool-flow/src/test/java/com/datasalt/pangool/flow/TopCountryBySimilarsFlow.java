package com.datasalt.pangool.flow;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRException;

/**
 * This is a somewhat more serious flow that executes 3 M/R steps in order to find the top country per each user
 * given a list of similarities between users and a user-country map. It shows the usage of {@link PangoolGrouperJob}.
 */
public class TopCountryBySimilarsFlow extends LinearFlow {

	/**
	 * Reads a list of pairwise similarities and emits the top N
	 */
	@SuppressWarnings("serial")
	public static class TopSimilarities extends PangoolGrouperJob {

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
			Schema similaritySchema = new Schema("similarity", Fields.parse("first:string, second:string, score:double"));

			// Define input processors (Mappers)
			bindInput("similarityInput", new TextInput(new SingleSchemaMapper<LongWritable, Text>(similaritySchema) {

				@Override
				public void map(LongWritable key, Text value, TupleMRContext context, Collector collector) throws IOException,
				    InterruptedException {
					String[] fields = value.toString().split("\t");
					tuple.set("first", fields[0].trim());
					tuple.set("second", fields[1].trim());
					tuple.set("score", Double.parseDouble(fields[2].trim()));
					collector.write(tuple);
				}
			}, similaritySchema));

			// Define the Reducer
			bindReducer(new SingleSchemaReducer<ITuple, NullWritable>(similaritySchema) {

				public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
				    throws IOException, InterruptedException, TupleMRException {

					Iterator<ITuple> iterator = tuples.iterator();
					for(int i = 0; i < n && iterator.hasNext(); i++) {
						Utils.shallowCopy(iterator.next(), tuple);
						collector.write(tuple, NullWritable.get());
					}
				};
			});

			bindOutput(new TupleOutput(similaritySchema));
		}
	}

	/**
	 * Attaches the country of the user in the right side of the relationship
	 */
	@SuppressWarnings("serial")
	public static class AttachCountryInfo extends PangoolGrouperJob {

		public AttachCountryInfo() {
			super("attachCountry", new Inputs("topSimilarities", "countryInfo"), Params.NONE, new NamedOutputs("unresolved"),
			    new GroupBy("second"));
		}

		@Override
		public void configure(Map<String, Object> parsedParameters) throws TupleMRException {
			// Define the schemas
			Schema similaritySchema = new Schema("similarity", Fields.parse("first:string, second:string, score:double"));
			Schema countryInfo = new Schema("countryInfo", Fields.parse("second:string, country:string"));
			Schema jointSchema = new Schema("jointSchema",
			    Fields.parse("first:string, second:string, score:double, country:string"));

			// Define the input processors (Mappers)
			bindInput("topSimilarities", new TupleInput(similaritySchema));
			bindInput("countryInfo", new TextInput(new SingleSchemaMapper<LongWritable, Text>(countryInfo) {

				@Override
				public void map(LongWritable key, Text value, TupleMRContext context, Collector collector) throws IOException,
				    InterruptedException {
					String[] fields = value.toString().split("\t");
					tuple.set("second", fields[0].trim());
					tuple.set("country", fields[1].trim());
					collector.write(tuple);
				}
			}, countryInfo));

			// Define the Reducer
			bindReducer(new SingleSchemaReducer<ITuple, NullWritable>(jointSchema) {
	
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
							collector.getNamedOutput("unresolved").write(new Text(tuple.get("second").toString()), NullWritable.get());
						}
					}
				};
			});
			
			bindOutput(new TupleOutput(jointSchema));
			bindOutput("unresolved", new TextOutput());
		}
	}

	/**
	 * Emits the top country for each user
	 */
	@SuppressWarnings("serial")
	public static class TopCountry extends PangoolGrouperJob {

		public TopCountry() {
	    super("topCountry", new Inputs("topSimilaritiesJoinCountryInfo"), Params.NONE, NamedOutputs.NONE,
			    new GroupBy("first"), new OrderBy().add("first", Order.ASC).add("score", Order.DESC));
    }

		@Override
    public void configure(Map<String, Object> parsedParameters) throws TupleMRException {
			// Define schemas
			Schema jointSchema = new Schema("jointSchema", Fields.parse("first:string, second:string, score:double, country:string"));
			Schema outSchema = new Schema("outSchema", Fields.parse("first:string, country:string"));
			
			// Define input processors (Mappers)
	    bindInput("topSimilaritiesJoinCountryInfo", new TupleInput(jointSchema));
			// Define Reducer
	    bindReducer(new SingleSchemaReducer<ITuple, NullWritable>(outSchema) {
	    	
				@Override
				public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
				    throws IOException, InterruptedException, TupleMRException {

					tuple.set("first", group.get("first").toString());
					tuple.set("country", tuples.iterator().next().get("country").toString());
					collector.write(tuple, NullWritable.get());
				}
	    });
	    
	    bindOutput(new TupleOutput(outSchema));
    }
	}
	
	public TopCountryBySimilarsFlow(String similarityFile, String countryInfoFile, Integer n, String output) throws TupleMRException {
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
