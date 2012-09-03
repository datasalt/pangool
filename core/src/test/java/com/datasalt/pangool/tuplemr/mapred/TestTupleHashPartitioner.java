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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.junit.Assert;
import org.junit.Test;

import com.datasalt.pangool.io.DatumWrapper;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.TupleMRConfig;
import com.datasalt.pangool.tuplemr.TupleMRConfigBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.utils.TestUtils;
import com.datasalt.pangool.utils.test.AbstractBaseTest;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestTupleHashPartitioner extends AbstractBaseTest{

	final static int MAX_ITERATIONS_OVER_ONE_SCHEMA = 100000;
	final static int N_PARTITIONS = 5;
	
  @Test
	public void multipleSourcesTest() throws TupleMRException, IOException {
		Configuration conf = getConf();
		TupleHashPartitioner partitioner = new TupleHashPartitioner();
		
		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("number1", Type.INT));
		fields.add(Field.create("string1", Type.STRING));
		fields.add(Field.create("string2", Type.STRING));
		Schema schema1 = new Schema("test1", fields);
		
		fields = new ArrayList<Field>();
		fields.add(Field.create("number1", Type.INT));
		fields.add(Field.create("string1", Type.STRING));
		fields.add(Field.create("number2", Type.LONG));
		Schema schema2 = new Schema("test2", fields);
		
		TupleMRConfigBuilder builder = new TupleMRConfigBuilder();
		builder.addIntermediateSchema(schema1);
		builder.addIntermediateSchema(schema2);
		builder.setGroupByFields("number1", "string1");
		TupleMRConfig tupleMRConf = builder.buildConf();
		TupleMRConfig.set(tupleMRConf, conf);
		
		partitioner.setConf(conf);
		
		ITuple tuple = new Tuple(schema1);
		tuple.set("number1", 35);
		tuple.set("string1", "foo");
		
		// Check that for the same prefix (number1, string1) we obtain the same partition
		
		int partitionId = -N_PARTITIONS;
		for(int i = 0; i < MAX_ITERATIONS_OVER_ONE_SCHEMA; i++) {
			tuple.set("string2", TestUtils.randomString(10));
			int thisPartitionId = partitioner.getPartition(new DatumWrapper(tuple), NullWritable.get(), N_PARTITIONS);
			Assert.assertTrue(thisPartitionId >= 0);
			Assert.assertTrue(thisPartitionId < N_PARTITIONS);
			if(partitionId == -N_PARTITIONS) {
				partitionId = thisPartitionId;
			} else {
				// Check that the returned partition is always the same even if "string2" field changes its value
				Assert.assertEquals(thisPartitionId, partitionId);
			}
		}
		
		// On the other hand, check that when we vary one of the group by fields, partition varies
		
		int partitionMatches[] = new int[N_PARTITIONS];
		for(int i = 0; i < MAX_ITERATIONS_OVER_ONE_SCHEMA; i++) {
			tuple.set("string1", TestUtils.randomString(10));
			int thisPartitionId = partitioner.getPartition(new DatumWrapper(tuple), NullWritable.get(), N_PARTITIONS);
			Assert.assertTrue(thisPartitionId >= 0);
			Assert.assertTrue(thisPartitionId < N_PARTITIONS);
			partitionMatches[thisPartitionId]++;;
		}
		
		for(int i = 0; i < partitionMatches.length; i++) {
			if(partitionMatches[i] == 0) {
				throw new AssertionError("Partition matches: 0 for partition " + i + ". Seems like a bug in the Partitioner.");
			}
		}
	}
	
	@Test
	public void sanityTest() throws TupleMRException, IOException {
		// This is a basic sanity test for checking that the partitioner works for nPartitions > 1
		
		Configuration conf = getConf();
		TupleHashPartitioner partitioner = new TupleHashPartitioner();

		List<Field> fields = new ArrayList<Field>();
		// We use one INT field - we'll put random numbers in it
		fields.add(Field.create("foo", Type.INT));
		Schema schema = new Schema("test", fields);
		
		TupleMRConfigBuilder builder = new TupleMRConfigBuilder();
		builder.addIntermediateSchema(schema);
		builder.setGroupByFields("foo");
		TupleMRConfig tupleMRConf = builder.buildConf();
		TupleMRConfig.set(tupleMRConf, conf);
		
		partitioner.setConf(conf);
		
		ITuple tuple = new Tuple(schema);
		
		int partitionMatches[] = new int[N_PARTITIONS];
		
		for(int i = 0; i < MAX_ITERATIONS_OVER_ONE_SCHEMA; i++) {
			tuple.set("foo", (int)(Math.random() * Integer.MAX_VALUE));
			int thisPartitionId = partitioner.getPartition(new DatumWrapper(tuple), NullWritable.get(), N_PARTITIONS);
			Assert.assertTrue(thisPartitionId >= 0);
			Assert.assertTrue(thisPartitionId < N_PARTITIONS);
			partitionMatches[thisPartitionId]++;;
		}
		
		for(int i = 0; i < partitionMatches.length; i++) {
			if(partitionMatches[i] == 0) {
				throw new AssertionError("Partition matches: 0 for partition " + i + ". Seems like a bug in the Partitioner.");
			}
		}
	}
}
