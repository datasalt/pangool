package com.datasalt.pangool.tuplemr.mapred.lib.input;

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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.HCatInputFormat;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Tuple;

/**
 * A bridge between HCatalog and Pangool that makes any HCatInputFormat compatible with Pangool. It delegates to
 * HCatInputFormat and returns a Pangool Tuple mapped from an HCatRecord.
 * <p>
 * See: http://incubator.apache.org/hcatalog/docs/r0.4.0/inputoutput.html
 */
@SuppressWarnings("serial")
public class HCatTupleInputFormat extends InputFormat<ITuple, NullWritable> implements Serializable {

	private HCatSchema schema;
	private Schema pangoolSchema;

	public HCatTupleInputFormat(String dbName, String tableName, Configuration conf) throws IOException {
		HCatInputFormat.setInput(conf, dbName, tableName);
		schema = HCatInputFormat.getTableSchema(conf);
		List<Field> pangoolSchemaFields = new ArrayList<Field>();
		for(HCatFieldSchema fieldSchema : schema.getFields()) {
			switch(fieldSchema.getType()) {
			case BIGINT:
				pangoolSchemaFields.add(Field.create(fieldSchema.getName(), Schema.Field.Type.LONG));
				break;
			case BOOLEAN:
				pangoolSchemaFields.add(Field.create(fieldSchema.getName(), Schema.Field.Type.BOOLEAN));
				break;
			case DOUBLE:
				pangoolSchemaFields.add(Field.create(fieldSchema.getName(), Schema.Field.Type.DOUBLE));
				break;
			case FLOAT:
				pangoolSchemaFields.add(Field.create(fieldSchema.getName(), Schema.Field.Type.FLOAT));
				break;
			case INT:
				pangoolSchemaFields.add(Field.create(fieldSchema.getName(), Schema.Field.Type.INT));
				break;
			case SMALLINT:
				pangoolSchemaFields.add(Field.create(fieldSchema.getName(), Schema.Field.Type.INT));
				break;
			case STRING:
				pangoolSchemaFields.add(Field.create(fieldSchema.getName(), Schema.Field.Type.STRING));
				break;
			case TINYINT:
				pangoolSchemaFields.add(Field.create(fieldSchema.getName(), Schema.Field.Type.INT));
				break;
			default:
				throw new IllegalArgumentException("Field type not supported (" + fieldSchema.getType()
				    + ") only primitive types can be bridged between HCatalog and Pangool.");
			}
		}
		// Instantiate a Pangool schema with the same name than the HCatalog table name
		this.pangoolSchema = new Schema(tableName, pangoolSchemaFields);
	}

	public HCatSchema getSchema() {
		return schema;
	}

	public Schema getPangoolSchema() {
		return pangoolSchema;
	}

	@Override
	public RecordReader<ITuple, NullWritable> createRecordReader(InputSplit split,
	    TaskAttemptContext taskContext) throws IOException, InterruptedException {

		HCatInputFormat iF = new HCatInputFormat();

		@SuppressWarnings("rawtypes")
		final RecordReader<WritableComparable, HCatRecord> hCatRecordReader = iF.createRecordReader(split,
		    taskContext);

		return new RecordReader<ITuple, NullWritable>() {

			ITuple tuple = new Tuple(pangoolSchema);

			@Override
			public void close() throws IOException {
				hCatRecordReader.close();
			}

			@Override
			public ITuple getCurrentKey() throws IOException, InterruptedException {
				HCatRecord record = hCatRecordReader.getCurrentValue();
				// Perform conversion between HCatRecord and Tuple
				for(int pos = 0; pos < schema.size(); pos++) {
					tuple.set(pos, record.get(pos));
				}
				return tuple;
			}

			@Override
			public NullWritable getCurrentValue() throws IOException, InterruptedException {
				return NullWritable.get();
			}

			@Override
			public float getProgress() throws IOException, InterruptedException {
				return hCatRecordReader.getProgress();
			}

			@Override
			public void initialize(InputSplit iS, TaskAttemptContext context) throws IOException,
			    InterruptedException {
				hCatRecordReader.initialize(iS, context);
			}

			@Override
			public boolean nextKeyValue() throws IOException, InterruptedException {
				return hCatRecordReader.nextKeyValue();
			}
		};
	}

	@Override
	public List<InputSplit> getSplits(JobContext jobcontext) throws IOException, InterruptedException {
		HCatInputFormat iF = new HCatInputFormat();
		return iF.getSplits(jobcontext);
	}
}
