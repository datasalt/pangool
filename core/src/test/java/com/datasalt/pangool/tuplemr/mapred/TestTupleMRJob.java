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
import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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
    for (int i = 0; i < NUM_ROWS_TO_GENERATE; i++) {
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
        for (ITuple tuple : tuples) {
          collector.write(fillTuple(true, tuple), NullWritable.get());
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
    readTuples(new Path(output + "/part-r-00000"), getConf(), new TupleVisitor() {

      @Override
      public void onTuple(ITuple tuple) {
        count.incrementAndGet();
      }
    });

    assertEquals(NUM_ROWS_TO_GENERATE, count.get());

    trash(input);
    trash(output);
  }

  @Test
  public void testJobWithNulls() throws IOException, TupleMRException, ClassNotFoundException, InterruptedException {
    Configuration conf = getConf();
    String input1 = TestTupleMRJob.class.getCanonicalName() + "-input1";
    String input2 = TestTupleMRJob.class.getCanonicalName() + "-input2";
    String output = TestTupleMRJob.class.getCanonicalName() + "-output";

    final Schema schemaNoNulls = new Schema("NoNulls", Fields.parse("f1:int,f2:string"));
    final Schema schemaNulls = new Schema("Nulls", Fields.parse("f1:int?,f2:string?"));
    Tuple t1 = new Tuple(schemaNoNulls);
    Tuple t2 = new Tuple(schemaNulls);

    t1.set(0, 0);
    t1.set(1, "nn");
    withTupleInput(input1, t1);

    Object tuples[][] = new Object[][]{
        new Object[]{0, null},
        new Object[]{0, "n1"},
        new Object[]{null, "n2"}
    };
    for (Object[] tuple : tuples) {
      t2.set(0, tuple[0]);
      t2.set(1, tuple[1]);
      withTupleInput(input2, t2);
    }

    TupleMRBuilder builder = new TupleMRBuilder(getConf(), "test");
    builder.addTupleInput(new Path(input1), new IdentityTupleMapper());
    builder.addTupleInput(new Path(input2), new IdentityTupleMapper());

    builder.setTupleReducer(new TupleReducer<ITuple, NullWritable>() {
      @Override
      public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector) throws IOException, InterruptedException, TupleMRException {
        int count = 0;
        for (ITuple tuple : tuples) {
          Tuple t = new Tuple(schemaNulls);
          t.set(0, tuple.get(0));
          t.set(1, tuple.get(1));
          collector.write(t, NullWritable.get());
          count++;
        }
        if (group.get(0) == null) {
          assertEquals(1, count);
        } else if (((Integer) group.get(0)) == 0) {
          assertEquals(3, count);
        }
      }
    });
    builder.addIntermediateSchema(schemaNoNulls);
    builder.addIntermediateSchema(schemaNulls);
    builder.setGroupByFields("f1");
    builder.setOrderBy(OrderBy.parse("f1:desc|null_smallest").addSchemaOrder(Criteria.Order.ASC));
    builder.setSpecificOrderBy("NoNulls", OrderBy.parse("f2:asc|null_biggest"));
    builder.setSpecificOrderBy("Nulls", OrderBy.parse("f2:asc|null_biggest"));
    builder.setTupleOutput(new Path(output), schemaNulls);

    Job job = builder.createJob();
    job.setNumReduceTasks(1);
    assertRun(job);

    final Object expectedOutput[][] = new Object[][]{
        new Object[]{0, "nn"},
        new Object[]{0, "n1"},
        new Object[]{0, null},
        new Object[]{null, "n2"}
    };

    boolean debug = false;
    if (debug) {
      readTuples(new Path(output + "/part-r-00000"), getConf(), new TupleVisitor() {
        @Override
        public void onTuple(ITuple t) {
          System.out.println(t);
        }
      });
    }

    readTuples(new Path(output + "/part-r-00000"), getConf(), new TupleVisitor() {
      int i = 0;

      @Override
      public void onTuple(ITuple t) {
        assertEqualsNull(expectedOutput[i][0], t.get(0));
        Object f2 = t.get(1);
        f2 = (f2 != null) ? f2.toString() : f2;
        assertEqualsNull(expectedOutput[i][1], f2);
        i++;
      }
    });

    trash(input1);
    trash(input2);
    trash(output);
  }

  private void assertEqualsNull(Object expected, Object actual) {
    if (expected == null) {
      assertNull(actual);
    } else {
      assertEquals(expected, actual);
    }
  }

}
