package com.datasalt.pangool.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

import com.datasalt.pangool.commons.HadoopUtils;
import com.datasalt.pangool.io.AvroUtils;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Tuple;

/**
 * Niceties and utilities for making Hadoop unit tests less painfully.
 */
public abstract class AbstractHadoopTestLibrary extends AbstractBaseTest {

	protected FileSystem fS;

	protected Map<String, List<Pair<Object, Object>>> outputs = new HashMap<String, List<Pair<Object, Object>>>();
	protected Map<String, SequenceFile.Writer> inputs = new HashMap<String, SequenceFile.Writer>();

	@Before
	public void initHadoop() throws IOException {
		// conf = getConf();
		fS = FileSystem.get(getConf());
	}

	@SuppressWarnings("rawtypes")
	private SequenceFile.Writer openWriter(String path, Class key, Class value) throws IOException {
		return new SequenceFile.Writer(fS, getConf(), new Path(path), key, value);
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
		}
		return null;
	}

	public void assertRun(Job job) throws IOException, InterruptedException, ClassNotFoundException {
		FileSystem fs = FileSystem.get(job.getConfiguration());
		HadoopUtils.deleteIfExists(fs, FileOutputFormat.getOutputPath(job));
		// Close input writers first
		for(Map.Entry<String, SequenceFile.Writer> entry : inputs.entrySet()) {
			entry.getValue().close();
		}
		job.waitForCompletion(true);
		Assert.assertTrue(job.isSuccessful());

	}

	public void cleanUp() throws IOException {
		for(Map.Entry<String, SequenceFile.Writer> entry : inputs.entrySet()) {
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
		SequenceFile.Writer writer = inputs.get(input);
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

	public void withOutput(String output, Object key) throws IOException, ClassNotFoundException, InstantiationException,
	    IllegalAccessException {
		withOutput(output, key, NullWritable.get());
	}

	public void withTupleOutput(String output, ITuple tuple) throws IOException {
		List<Pair<Object, Object>> outs = ensureTupleOutput(output);
		for(Pair<Object, Object> out: outs) {
			ITuple theTuple = (ITuple)out.getFirst();
			if(theTuple.equals(tuple)) {
				return;
			}
		}
		/*
		 * Not found. Let's create some meaningful message
		 */
		if(outs.size() == 0) {
			throw new AssertionError("Empty output " + output);
		}
		System.err.println("Not found in output. Tuple: " + tuple);
		for(Pair<Object, Object> inOutput : outs) {
			System.err.println("Output entry -> Tuple: " + inOutput.getFirst());
		}
		throw new AssertionError("Not found in output -> Tuple: " + tuple);
	}

	public void withOutput(String output, Object key, Object value) throws IOException, ClassNotFoundException,
	    InstantiationException, IllegalAccessException {
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

	public List<Pair<Object, Object>> ensureTupleOutput(String output) throws IOException {
		List<Pair<Object, Object>> outs = outputs.get(output);
		if(outs == null) {
			outs = new ArrayList<Pair<Object, Object>>();
			SpecificDatumReader<Record> specificReader = new SpecificDatumReader<Record>();
			FsInput fSInput = new FsInput(new Path(output), getConf());
			FileReader<Record> reader = DataFileReader.openReader(fSInput, specificReader);
			AvroWrapper<Record> wrapper = new AvroWrapper<Record>();
			while(reader.hasNext()) {
				wrapper.datum(reader.next(wrapper.datum()));
				Tuple tuple = new Tuple();
				AvroUtils.toTuple(wrapper.datum(), tuple, reader.getSchema());
				outs.add(new Pair<Object, Object>(tuple, NullWritable.get()));
			}
			reader.close();
		}

		return outs;
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
