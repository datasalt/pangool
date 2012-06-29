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

import com.datasalt.pangool.PangoolRuntimeException;
import com.datasalt.pangool.flow.io.TextInput;
import com.datasalt.pangool.flow.io.TupleInput;
import com.datasalt.pangool.flow.io.TupleOutput;
import com.datasalt.pangool.flow.mapred.SingleSchemaReducer;
import com.datasalt.pangool.flow.mapred.TextMapper;
import com.datasalt.pangool.flow.ops.TupleParser;
import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Utf8;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRException;

/**
 * This job takes 2 inputs :
 * 
 * student_scores: (student,score) PK(student)
 * 
 * student_info (student,age,country) PK(student)
 * 
 * This example calculates the average,variance and standard deviation of the scores for every country, only for
 * students older than 15 years old.
 * 
 * The flow is composed by 3 jobs :
 * 
 * 1st job: ======= Inputs : student_scores student_info Task : filter only the students older than 15 years old, join
 * the two datasets grouping by "country" Output : (country,score)
 * 
 * 2nd job: Inputs : (country,score) <= output from 1st job Task: just compute the average for every country, grouping
 * by country Output : (country,average)
 * 
 * 3rd job: Inputs: - (country,average) <= output from 2nd job - (country,score) <= output from 1st job Task : calculate
 * the variance, and deviation Output : (country,average,variance,stdev)
 * 
 */
@SuppressWarnings("serial")
public class StandardDeviationFlow extends LinearFlow {

	public StandardDeviationFlow(String studentsFile, String scoresFile, String output, int minAge)
	    throws TupleMRException {

		// Define schemas
		final Schema studentSchema = new Schema("students", Fields.parse("student:string, age:int,country:string"));
		final Schema scoresSchema = new Schema("scores", Fields.parse("student:string,score:double"));
		final Schema countryScoresSchema = new Schema("country_scores", Fields.parse("country:string, score:double"));
		final Schema countryAveragesSchema = new Schema("country_averages", Fields.parse("country:string,average:double"));
		final Schema finalOutSchema = new Schema("finalOutSchema",
		    Fields.parse("country:string,average:double,variance:double,stdev:double"));

		// Define the Flow
		// new Params().add(new Param("minAge"))
		add(new FlowMR("job1", new Inputs("students", "scores"), new Params(new Param("minAge", Integer.class)),
		    NamedOutputs.NONE, new GroupBy("student"), null) {
			int minAge;

			@Override
			public void configure(Map<String, Object> parameters) throws TupleMRException {
				this.minAge = (Integer) parameters.get("minAge");
				// Define input processors (Mappers)
				addInput("students", new TextInput(new TextMapper(new TupleParser(studentSchema, "\\s+")), studentSchema));
				addInput("scores", new TextInput(new TextMapper(new TupleParser(scoresSchema, "\\s+")), scoresSchema));
				// Define the Reducer
				setReducer(new SingleSchemaReducer(countryScoresSchema) {

					public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
					    throws IOException, InterruptedException, TupleMRException {
						Double score = null;
						Utf8 country = null;

						for(ITuple tuple : tuples) {
							if("students".equals(tuple.getSchema().getName())) {
								int age = (Integer) tuple.get("age");
								if(age < minAge) {
									return;
								}
								country = (Utf8) tuple.get("country");
							} else {
								score = (Double) tuple.get("score");
							}
						}

						ITuple outTuple = getOutputTuple();
						outTuple.set("country", country);
						outTuple.set("score", score);
						collector.write(outTuple, NullWritable.get());
					}
				});

				setOutput(new TupleOutput(countryScoresSchema));
			}
		});

		add(new FlowMR("job2", new Inputs("input"), Params.NONE, NamedOutputs.NONE, new GroupBy("country"), null) {

			@Override
			public void configure(Map<String, Object> parameters) throws TupleMRException {
				// Define input processors (Mappers)
				addInput("input", new TupleInput(countryScoresSchema));

				setReducer(new SingleSchemaReducer(countryAveragesSchema) {

					public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
					    throws IOException, InterruptedException, TupleMRException {
						int numScores = 0;
						double sumScores = 0.0f;
						for(ITuple tuple : tuples) {
							numScores++;
							sumScores += (Double) tuple.get("score");
						}
						ITuple outTuple = getOutputTuple();
						outTuple.set("country", group.get("country"));
						outTuple.set("average", sumScores / (double) numScores);
						collector.write(outTuple, NullWritable.get());
					}
				});

				setOutput(new TupleOutput(countryAveragesSchema));
				// setOutput(new TextOutput());
			}
		});

		add(new FlowMR("job3", new Inputs("country_averages", "country_scores"), Params.NONE, NamedOutputs.NONE,
		    new GroupBy("country"), new OrderBy().add("country", Order.ASC).addSchemaOrder(Order.ASC)) {

			@Override
			public void configure(Map<String, Object> parameters) throws TupleMRException {
				// Define the input processors (Mappers)
				addInput("country_averages", new TupleInput(countryAveragesSchema));
				addInput("country_scores", new TupleInput(countryScoresSchema));
				// Define the Reducer - this is a custom reducer
				setReducer(new SingleSchemaReducer(finalOutSchema) {

					@Override
					public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
					    throws IOException, InterruptedException, TupleMRException {
						Double average = null;
						Utf8 country = (Utf8) group.get("country");
						int numScores = 0;
						double sumDiff = 0.0f;
						for(ITuple tuple : tuples) {
							if("country_averages".equals(tuple.getSchema().getName())) {
								average = ((Double) tuple.get("average"));
							} else if("country_scores".equals(tuple.getSchema().getName())) {
								double score = (Double) tuple.get("score");
								numScores++;
								sumDiff += (score - average) * (score - average);
							} else {
								throw new PangoolRuntimeException("Unknown schema : " + tuple.getSchema().getName());
							}
						}
						double variance = sumDiff / numScores;
						ITuple outTuple = getOutputTuple();
						outTuple.set("country", country);
						outTuple.set("average", average);
						outTuple.set("variance", variance);
						outTuple.set("stdev", Math.sqrt(variance));
						collector.write(outTuple, NullWritable.get());
					}
				});
				setOutput(new TupleOutput(finalOutSchema));
				// setOutput(new TextOutput());
			}
		});

		add(studentsFile);
		add(scoresFile);

		bind("job1.minAge", minAge);
		// this creates (country, score) entries..
		bind("job1.students", studentsFile);
		bind("job1.scores", scoresFile);

		// this creates (country , average)
		bind("job2.input", "job1.output");

		// creates (country,average,variance,stdev);
		bind("job3.country_scores", "job1.output");
		bind("job3.country_averages", "job2.output");
		bind("job3.output", output);

	}
}