/**
 * Copyright [2012] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasalt.pangool.flow;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;

import com.datasalt.pangool.flow.io.TextInput;
import com.datasalt.pangool.flow.io.TextOutput;
import com.datasalt.pangool.flow.io.TextSer;
import com.datasalt.pangool.flow.io.TupleInput;
import com.datasalt.pangool.flow.io.TupleOutput;
import com.datasalt.pangool.flow.mapred.SingleSchemaReducer;
import com.datasalt.pangool.flow.mapred.TextMapper;
import com.datasalt.pangool.flow.mapred.TupleOpReducer;
import com.datasalt.pangool.flow.ops.ChainOp;
import com.datasalt.pangool.flow.ops.FirstTuple;
import com.datasalt.pangool.flow.ops.SubsetCopy;
import com.datasalt.pangool.flow.ops.TopTuples;
import com.datasalt.pangool.flow.ops.TupleParser;
import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRException;

/**
 * This is a somewhat more serious flow that executes 3 M/R steps in order to find the top country per each user given a
 * list of similarities between users and a user-country map. It shows the usage of {@link MapReduceStep}.
 */
@SuppressWarnings("serial")
public class TopCountryBySimilarsFlow extends BaseFlow {

	public TopCountryBySimilarsFlow(String similarityFile, String countryInfoFile, Integer topSize, String output)
	    throws TupleMRException {

		// Define schemas
		final Schema similaritySchema = new Schema("similarity", Fields.parse("first:string, second:string, score:double"));
		final Schema countryInfo = new Schema("countryInfo", Fields.parse("second:string, country:string"));
		final Schema jointSchema = new Schema("jointSchema",
		    Fields.parse("first:string, second:string, score:double, country:string"));
		final Schema outSchema = new Schema("outSchema", Fields.parse("first:string, country:string"));

		// Define the Flow

		// Emit the top "n" user similarities for each user
		add(new MapReduceStep("topSimilarities", new Inputs("similarityInput"), new Params(new Param("topSize", Integer.class)),
		    NamedOutputs.NONE, new GroupBy("first"), new OrderBy().add("first", Order.ASC).add("score", Order.DESC)) {

			@Override
			public void configure(Map<String, Object> parameters) throws TupleMRException {
				// Read the "n" parameter. This is the size of the top.
				int topSize = (Integer) parameters.get("topSize");
				// Define input processors (Mappers)
				addInput("similarityInput", new TextInput(new TextMapper(new TupleParser(similaritySchema, "\t"))));
				// Define the Reducer
				setReducer(new TupleOpReducer(new TopTuples(topSize), similaritySchema));
				setOutput(new TupleOutput(similaritySchema));
			}
		});

		add(new MapReduceStep("attachCountry", new Inputs("topSimilarities", "countryInfo"), Params.NONE, new NamedOutputs(
		    "unresolved"), new GroupBy("second"),new OrderBy().add("second",Order.ASC).addSchemaOrder(Order.DESC)) {

			@Override
			public void configure(Map<String, Object> parameters) throws TupleMRException {
				// Define the input processors (Mappers)
				addInput("topSimilarities", new TupleInput(similaritySchema));
				addInput("countryInfo", new TextInput(new TextMapper(new TupleParser(countryInfo, "\t"))));
				// Define the Reducer - this is a custom reducer
				setReducer(new SingleSchemaReducer(jointSchema) {

					TextSer unresolvedId = new TextSer();

					@Override
					public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
					    throws IOException, InterruptedException, TupleMRException {

						String country = null;
						for(ITuple tuple : tuples) {
							if(tuple.getSchema().getName().equals("countryInfo")) {
								country = tuple.get("country").toString();
							} else if(country != null) {
								Utils.shallowCopy(tuple, getOutputTuple(), "first", "second", "score");
								getOutputTuple().set("country", country);
								collector.write(getOutputTuple(), NullWritable.get());
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
		});

		add(new MapReduceStep("topCountry", new Inputs("topSimilaritiesJoinCountryInfo"), Params.NONE, NamedOutputs.NONE,
		    new GroupBy("first"), new OrderBy().add("first", Order.ASC).add("score", Order.DESC)) {
			@Override
			public void configure(Map<String, Object> parameters) throws TupleMRException {
				// Define input processors (Mappers)
				addInput("topSimilaritiesJoinCountryInfo", new TupleInput(jointSchema));
				// Define Reducer
				setReducer(new TupleOpReducer(
				    new ChainOp<Iterable<ITuple>, ITuple>(new FirstTuple(), new SubsetCopy(outSchema)), outSchema));
				setOutput(new TupleOutput(outSchema));
			}
		});

		add(similarityFile);
		add(countryInfoFile);

		bind("topSimilarities.topSize", topSize);
		bind("topSimilarities.similarityInput", similarityFile);
		bind("attachCountry.topSimilarities", "topSimilarities.output");
		bind("attachCountry.countryInfo", countryInfoFile);
		bind("topCountry.topSimilaritiesJoinCountryInfo", "attachCountry.output");
		bind("topCountry.output", output);
	}
}