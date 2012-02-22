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
package com.datasalt.pangool.examples.movingaverage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.datasalt.pangool.cogroup.CoGrouper;
import com.datasalt.pangool.cogroup.CoGrouperException;
import com.datasalt.pangool.cogroup.processors.GroupHandler;
import com.datasalt.pangool.cogroup.processors.InputProcessor;
import com.datasalt.pangool.cogroup.sorting.Criteria.Order;
import com.datasalt.pangool.cogroup.sorting.SortBy;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Schema;
import com.datasalt.pangool.io.tuple.Schema.Field;
import com.datasalt.pangool.io.tuple.Tuple;
import com.datasalt.pangool.test.Pair;

/**
 * In this example we are calculating moving averages by a configurable number of days over an schema like this: ["url", "date", "visits"].
 * We want to calculate, for each URL, the moving average for "n" days of unique visits for all the dates we have.
 */
public class MovingAverage {

	@SuppressWarnings("serial")
	private static class URLVisitsProcessor extends InputProcessor<LongWritable, Text> {

		private Schema schema;

		public void setup(CoGrouperContext context, Collector collector) throws IOException, InterruptedException {
			this.schema = context.getCoGrouperConfig().getSourceSchema("my_schema");
		}

		@Override
		public void process(LongWritable key, Text value, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException {
			
			// Just parsing the text input and emitting a Tuple
			Tuple tuple = new Tuple(schema);
			String[] fields = value.toString().trim().split("\t");
			tuple.set("url", fields[0]);
			tuple.set("date", fields[1]);
			tuple.set("visits", Integer.parseInt(fields[2]));
			collector.write(tuple);
		}
	}

	@SuppressWarnings("serial")
	public static class MovingAverageHandler extends GroupHandler<Text, NullWritable> {

		int nDaysAverage;
		Queue<Pair<Integer, DateTime>> dataPoints = new LinkedList<Pair<Integer, DateTime>>();
		private final static DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd");

		public MovingAverageHandler(int nDaysAverage) { // Configurable number of moving average days
			this.nDaysAverage = nDaysAverage;
		}

		@Override
		public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException, CoGrouperException {

			dataPoints.clear(); 

			for(ITuple tuple : tuples) {
				String currentDate = tuple.get("date").toString();
				DateTime date = format.parseDateTime(currentDate);
				// Add current data point to the window
				dataPoints.add(new Pair<Integer, DateTime>((Integer) tuple.get("visits"), date));
				// Adjust window to desired date
				DateTime desiredDate = date.plusDays(-(nDaysAverage - 1));
				Pair<Integer, DateTime> lastDataPoint = dataPoints.peek();
				while(lastDataPoint.getSecond().isBefore(desiredDate)) {
					dataPoints.poll();
					lastDataPoint = dataPoints.peek();
				}
				// Calculate current average and emit
				int average = 0;
				for(Pair<Integer, DateTime> dataPoint: dataPoints) {
					average += dataPoint.getFirst();
				}
				double avg = average / (double)dataPoints.size();
				collector.write(new Text(tuple.get("url") + "\t" + currentDate + "\t" + avg), NullWritable.get());
			}
		}
	}

	public Job getJob(Configuration conf, String input, String output, int nDaysAverage) throws CoGrouperException,
	    IOException {
		// Configure schema, sort and group by
		List<Field> fields = new ArrayList<Field>();
		fields.add(new Field("url", String.class));
		fields.add(new Field("date", String.class));
		fields.add(new Field("visits", Integer.class));

		Schema schema = new Schema("my_schema", fields);

		CoGrouper grouper = new CoGrouper(conf);
		grouper.addSourceSchema(schema);
		grouper.setGroupByFields("url");
		grouper.setOrderBy(new SortBy().add("url", Order.ASC).add("date", Order.ASC));
		// Input / output and such
		grouper.setGroupHandler(new MovingAverageHandler(nDaysAverage));
		grouper.setOutput(new Path(output), TextOutputFormat.class, Text.class, NullWritable.class);
		grouper.addInput(new Path(input), TextInputFormat.class, new URLVisitsProcessor());
		return grouper.createJob();
	}
}
