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

package com.datasalt.pangool.tuplemr.mapred;

import com.datasalt.pangool.BaseTest;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Just tests full executions of a complete job.
 */
public class TestTupleMRJob extends BaseTest implements Serializable {

  @Test
  public void testFillingTuplesJob() throws IOException, ClassNotFoundException, InterruptedException, TupleMRException {
    int NUM_ROWS_TO_GENERATE = 100;

    Configuration conf = getConf();
    String input = TestTupleMRJob.class + "-input";
    String output = TestTupleMRJob.class + "-output";

    ITuple tuple = new Tuple(SCHEMA);
    for (int i = 0; i<NUM_ROWS_TO_GENERATE; i++) {
      withTupleInput(input, fillTuple(true, tuple));
    }

    TupleMRBuilder builder = new TupleMRBuilder(getConf(), "test");
    builder.addTupleInput(new Path(input), new TupleMapper<ITuple, NullWritable>() {

      @Override
      public void map(ITuple iTuple, NullWritable nullWritable, TupleMRContext context, Collector collector) throws IOException, InterruptedException {
        collector.write(fillTuple(true, iTuple));
      }
    });
    builder.setTupleReducer(new TupleReducer<ITuple, NullWritable>() {
      @Override
      public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector) throws IOException, InterruptedException, TupleMRException {
        for(ITuple tuple: tuples) {
          collector.write(fillTuple(true,tuple), NullWritable.get());
        }
      }
    });
    builder.addIntermediateSchema(SCHEMA);
    builder.setGroupByFields(SCHEMA.getField(0).getName());
    builder.setTupleOutput(new Path(output), SCHEMA);


    Job job = builder.createJob();
    job.setNumReduceTasks(1);
    assertRun(job);

    final AtomicInteger count = new AtomicInteger();
    readTuples(new Path(output+ "/part-r-00000"), getConf(), new TupleVisitor() {

      @Override
      public void onTuple(ITuple tuple) {
        count.incrementAndGet();
      }
    });

    assertEquals(NUM_ROWS_TO_GENERATE, count.get());

    trash(input);
    trash(output);
  }

}
