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
package com.datasalt.pangool.examples.gameoflife;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.mortbay.log.Log;

import com.datasalt.pangool.examples.BaseExampleJob;
import com.datasalt.pangool.examples.gameoflife.GameOfLife.GameOfLifeException;
import com.datasalt.pangool.examples.gameoflife.GameOfLife.GameOfLifeException.CauseMessage;
import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.HadoopOutputFormat;

@SuppressWarnings("serial")
/**
 * A M/R Job that uses Pangool to run a big amount of GOL (http://en.wikipedia.org/wiki/The_Game_of_Life) simulations
 * The user enters a grid n size (nxn) and the M/R calculates all the simulations belonging to all possible
 * configurations in the grid. The job outputs the configurations that reached convergence together with the
 * number of iterations it took for reaching it.
 * <p>
 * The outputs are arrays of bytes that need to be read right-to-left.
 * For example, the array [0, 0, 0, 0, 0, 0, 7] (right-to-left) is actually the (left-to-right) bit sequence 111000000...
 * Bit sequences are transformed into GOL configurations depending on the grid size. 
 * For a grid size of 2, 1110 would represent a matrix whose first row is 11 and second row 10.
 * <p>
 * Makes use of {@link GameOfLife} Java class.
 */
public class GameOfLifeJob extends BaseExampleJob implements Serializable {

	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 3) {
			failArguments("Wrong number of arguments");
			return -1;
		}
		String output = args[0];
		String input = GameOfLifeJob.class.getName() + "-prepared-input";
		delete(output);
		delete(input);

		final int gridSize = Integer.parseInt(args[1]);
		// Write the input of the job as a set of (min, max) intervals
		// Each number between (min, max) represents a possible initial configuration for Game of Life
		int parallelism = Integer.parseInt(args[2]);
		int maxCombinations = (int) Math.pow(2, gridSize * gridSize);
		int splitSize = maxCombinations / parallelism;
		FileSystem fS = FileSystem.get(conf);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fS.create(new Path(input))));
		for(int i = 0; i < parallelism; i++) {
			writer.write(((i * splitSize) + 1) + "\t" + ((i + 1) * splitSize) + "\n");
		}
		writer.close();

		// Optional parameters: maxX, maxY, #iterations
		final int maxX = conf.getInt("gol.max_x", 32);
		final int maxY = conf.getInt("gol.max_y", 32);
		final int iterations = conf.getInt("gol.iterations", 1000);
		Log.info("using parameters: maxX grid: " + maxX + " maxY grid: " + maxY + " max #iterations: " + iterations);
		
		// Define the intermediate schema: a pair of ints
		final Schema schema = new Schema("minMax", Fields.parse("min:int, max:int"));

		TupleMRBuilder job = new TupleMRBuilder(conf);
		job.addIntermediateSchema(schema);
		job.setGroupByFields("min", "max");
		job.setCustomPartitionFields("min");
		// Define the input and its associated mapper
		// The mapper will just emit the (min, max) pairs to the reduce stage
		job.addInput(new Path(input), new HadoopInputFormat(TextInputFormat.class), new TupleMapper<LongWritable, Text>() {

			Tuple tuple = new Tuple(schema);

			@Override
			public void map(LongWritable key, Text value, TupleMRContext context, Collector collector) throws IOException,
			    InterruptedException {
				String[] fields = value.toString().split("\t");
				tuple.set("min", Integer.parseInt(fields[0]));
				tuple.set("max", Integer.parseInt(fields[1]));
				collector.write(tuple);
			}
		});

		// Define the reducer
		// The reducer will run as many games of life as (max - min) for each interval it receives
		// It will emit the inputs of GOL that converged together with the number of iterations
		// Note that inputs that produce grid overflow are ignored (but may have longer iteration convergence)
		job.setTupleReducer(new TupleReducer<Text, NullWritable>() {

			public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
			    throws IOException, InterruptedException, TupleMRException {

				int min = (Integer) group.get("min"), max = (Integer) group.get("max");
				for(int i = min; i < max; i++) {
					try {
						GameOfLife gameOfLife = new GameOfLife(gridSize, GameOfLife.longToBytes((long) i), maxX, maxY, iterations);
						while(true) {
							gameOfLife.nextCycle();
						}
					} catch(GameOfLifeException e) {
						context.getHadoopContext().progress();
						context.getHadoopContext().getCounter("stats", e.getCauseMessage() + "").increment(1);
						if(e.getCauseMessage().equals(CauseMessage.CONVERGENCE_REACHED)) {
							collector.write(new Text(Arrays.toString(GameOfLife.longToBytes((long) i)) + "\t" + e.getIterations()),
							    NullWritable.get());
						}
					}
				}
			};
		});

		job.setOutput(new Path(output), new HadoopOutputFormat(TextOutputFormat.class), Text.class, NullWritable.class);
		job.createJob().waitForCompletion(true);
		delete(input);
		return 0;
	}

	public GameOfLifeJob() {
		// 1) Output of the job
		// 2) Size n of the grid (nxn) from where we will calculate all possible inputs to GOL
		// 3) The amount of parallelism (number of splits that will be created). Normally = n. reducers.
		super("Usage: [output-path] [input-grid-size] [parallelism]");
	}

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new GameOfLifeJob(), args);
	}
}