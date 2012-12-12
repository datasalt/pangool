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
package com.datasalt.pangool.utils.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.io.TupleFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Assert;
import org.junit.Before;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.utils.HadoopUtils;
import com.datasalt.pangool.utils.Pair;

/**
 * Niceties and utilities for making Hadoop unit tests less painfully.
 */
public abstract class AbstractHadoopTestLibrary extends AbstractBaseTest {

	protected FileSystem fS;

	protected Map<String, List<Pair<Object, Object>>> outputs = new HashMap<String, List<Pair<Object, Object>>>();
	protected Map<String, Object> inputs = new HashMap<String, Object>();

	@Before
	public void initHadoop() throws IOException {
		fS = FileSystem.get(getConf());
	}

	@SuppressWarnings("rawtypes")
	private SequenceFile.Writer openWriter(String path, Class key, Class value) throws IOException {
		return new SequenceFile.Writer(fS, getConf(), new Path(path), key, value);
	}

  private TupleFile.Writer openTupleWriter(String path, Schema schema) throws IOException {
    return new TupleFile.Writer(fS, getConf(), new Path(path), schema);
  }

	public Writable writable(Object obj) {
		if(obj instanceof Integer) {
			return new IntWritable((Integer) obj);
		} else if(obj instanceof Double) {
			return new DoubleWritable((Double) obj);
		} else if(obj instanceof Long) {
			return new LongWritable((Long) obj);
		} else if(obj instanceof String) {
			return new Text((String) obj);
		} else if(obj instanceof Float) {
			return new FloatWritable((Float)obj);
		} else if(obj instanceof Boolean){
			return new BooleanWritable((Boolean)obj);
		}
		return null;
	}

	public void assertRun(Job job) throws IOException, InterruptedException, ClassNotFoundException {
		FileSystem fs = FileSystem.get(job.getConfiguration());
		HadoopUtils.deleteIfExists(fs, FileOutputFormat.getOutputPath(job));
		// Close input writers first
		for(Map.Entry<String, Object> entry : inputs.entrySet()) {
      Object in = entry.getValue();
      if (in instanceof SequenceFile.Writer) {
         ((SequenceFile.Writer) in).close();
      } else if (in instanceof TupleFile.Writer) {
        ((TupleFile.Writer) in).close();
      }
		}
		job.waitForCompletion(true);
		Assert.assertTrue(job.isSuccessful());

	}

	public void cleanUp() throws IOException {
		for(Map.Entry<String, Object> entry : inputs.entrySet()) {
			trash(entry.getKey());
		}
		for(Map.Entry<String, List<Pair<Object, Object>>> entry : outputs.entrySet()) {
			Path p = new Path(entry.getKey());
			if(p.toString().contains("-0000")) {
				p = p.getParent();
			}
			trash(p.toString());
		}
	}

	protected void trash(String... folders) throws IOException {
		for(String folder : folders) {
			HadoopUtils.deleteIfExists(fS, new Path(folder));
		}
	}

	protected String firstReducerOutput(String path) {
		return path + "/part-r-00000";
	}

	protected String firstMapOutput(String path) {
		return path + "/part-m-00000";
	}

	protected String firstReducerMultiOutput(String path, String multiOutputName) {
		return path + "/" + multiOutputName + "-r-00000";
	}

	protected String firstMapperMultiOutput(String path, String multiOutputName) {
		return path + "/" + multiOutputName + "-m-00000";
	}

	public AbstractHadoopTestLibrary withInput(String input, Object key, Object value) throws IOException {
		SequenceFile.Writer writer = (SequenceFile.Writer) inputs.get(input);
		if(writer == null) {
			writer = openWriter(input, key.getClass(), value.getClass());
			inputs.put(input, writer);
		}
		writer.append(key, value);
		return this;
	}

	public AbstractHadoopTestLibrary withInput(String input, Object key) throws IOException {
		return withInput(input, key, NullWritable.get());
	}

  public AbstractHadoopTestLibrary withTupleInput(String input, ITuple tuple) throws IOException {
    TupleFile.Writer writer = (TupleFile.Writer) inputs.get(input);
    if(writer == null) {
      writer = openTupleWriter(input, tuple.getSchema());
      inputs.put(input, writer);
    }
    writer.append(tuple);
    return this;
  }

	public void withOutput(String output, Object key) throws IOException, ClassNotFoundException, InstantiationException,
	    IllegalAccessException {
		withOutput(output, key, NullWritable.get());
	}

	public abstract static class TupleVisitor {

		public abstract void onTuple(ITuple tuple);
	}

	public static class PrintVisitor extends TupleVisitor {

		@Override
    public void onTuple(ITuple tuple) {
	    System.out.println(tuple);
    }
	}
	
	/*
	 * Read the Tuples from a TupleOutput using TupleInputReader.
	 */
	public static void readTuples(Path file, Configuration conf, TupleVisitor iterator) throws IOException, InterruptedException {
    TupleFile.Reader reader = new TupleFile.Reader(FileSystem.get(file.toUri(), conf), conf, file);
    Tuple tuple = new Tuple(reader.getSchema());
		while(reader.next(tuple)) {
			iterator.onTuple(tuple);
		}
		reader.close();
	}
	
	public void withTupleOutput(String output, final ITuple expectedTuple) throws IOException, InterruptedException {
		final AtomicBoolean found = new AtomicBoolean(false);
		final AtomicInteger tuples = new AtomicInteger(0); 
		readTuples(new Path(output), new Configuration(), new TupleVisitor() {
			@Override
      public void onTuple(ITuple tuple) {
	      tuples.incrementAndGet();
	      if(tuple.equals(expectedTuple)) {
	      	found.set(true);
	      }
      }
		});
		
		if(found.get()) {
			return;
		}

		/*
		 * Not found. Let's create some meaningful message
		 */
		if(tuples.get() == 0) {
			throw new AssertionError("Empty output " + output);
		}
		System.err.println("Not found in output. Tuple: " + expectedTuple);
		readTuples(new Path(output), new Configuration(), new TupleVisitor() {
			public void onTuple(ITuple tuple) {
				System.err.println("Output entry -> Tuple: " + tuple);
			};
		});
		
		throw new AssertionError("Not found in output -> Tuple: " + expectedTuple);
	}

	public void withOutput(String output, Object key, Object value) throws IOException {
		List<Pair<Object, Object>> outs = ensureOutput(output);
		for(Pair<Object, Object> inOutput : outs) {
			if(inOutput.getFirst().equals(key) && inOutput.getSecond().equals(value)) {
				return;
			}
		}
		/*
		 * Not found. Let's create some meaningful message
		 */
		if(outs.size() == 0) {
			throw new AssertionError("Empty output " + output);
		}
		System.err.println("Not found in output. KEY: " + key + ", VALUE: " + value);
		for(Pair<Object, Object> inOutput : outs) {
			System.err.println("Output entry -> KEY: " + inOutput.getFirst() + ", VALUE: " + inOutput.getSecond());
		}
		throw new AssertionError("Not found in output -> KEY: " + key + ", VALUE: " + value);
	}

	public List<Pair<Object, Object>> ensureOutput(String output) throws IOException {
		List<Pair<Object, Object>> outs = outputs.get(output);
		if(outs == null) {
			outs = new ArrayList<Pair<Object, Object>>();
			SequenceFile.Reader reader = new SequenceFile.Reader(fS, new Path(output), getConf());
			Object keyToRead, valueToRead;
			keyToRead = ReflectionUtils.newInstance(reader.getKeyClass(), getConf());
			valueToRead = ReflectionUtils.newInstance(reader.getValueClass(), getConf());

			while(reader.next(keyToRead) != null) {
				valueToRead = reader.getCurrentValue(valueToRead);
				Pair<Object, Object> p = new Pair<Object, Object>(keyToRead, valueToRead);
				outs.add(p);
				keyToRead = ReflectionUtils.newInstance(reader.getKeyClass(), getConf());
				valueToRead = ReflectionUtils.newInstance(reader.getValueClass(), getConf());
			}
			reader.close();
			outputs.put(output, outs);
		}
		return outs;
	}

	/**
	 * Dumps to string the given output
	 */
	public void dumpOutput(String output) throws IOException {
		List<Pair<Object, Object>> outs = ensureOutput(output);
		for(Pair<Object, Object> inOutput : outs) {
			System.out.println("KEY: " + inOutput.getFirst() + ", VALUE: " + inOutput.getSecond());
		}
	}

}
