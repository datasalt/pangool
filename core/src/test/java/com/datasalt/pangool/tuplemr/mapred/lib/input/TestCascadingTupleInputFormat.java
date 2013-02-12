package com.datasalt.pangool.tuplemr.mapred.lib.input;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.tuplemr.MapOnlyJobBuilder;
import com.datasalt.pangool.tuplemr.mapred.MapOnlyMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.output.HadoopOutputFormat;
import com.datasalt.pangool.utils.HadoopUtils;
import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;
import com.google.common.io.Files;

@SuppressWarnings("serial")
public class TestCascadingTupleInputFormat extends AbstractHadoopTestLibrary implements Serializable {

	public final static String OUTPUT = "out-" + TestCascadingTupleInputFormat.class.getName();

	@Test
	public void test() throws Exception {
		MapOnlyJobBuilder builder = new MapOnlyJobBuilder(getConf());
		// Enable Cascading serialization in Hadoop config.
		CascadingTupleInputFormat.setSerializations(getConf());
		// Instantiate InputFormat
		InputFormat<ITuple, NullWritable> iF = new CascadingTupleInputFormat("logs", "day", "month", "year",
		    "count", "metric", "value");
		builder.addInput(new Path("src/test/resources/cascading-binary"), iF,
		    new MapOnlyMapper<ITuple, NullWritable, Text, NullWritable>() {

			    @Override
			    protected void map(ITuple key, NullWritable value, Context context) throws IOException,
			        InterruptedException {
				    context.write(new Text(key.toString()), NullWritable.get());
			    }
		    });

		builder.setOutput(new Path(OUTPUT), new HadoopOutputFormat(TextOutputFormat.class), Text.class,
		    NullWritable.class);
		Job job = builder.createJob();
		try {
			assertRun(job);
		} finally {
			builder.cleanUpInstanceFiles();
		}

		String expectedOutput = "{\"day\":20,\"month\":10,\"year\":2012,\"count\":97,\"metric\":\"ALL\",\"value\":\"\"}\n"
		    + "{\"day\":21,\"month\":10,\"year\":2012,\"count\":717,\"metric\":\"ALL\",\"value\":\"\"}\n"
		    + "{\"day\":22,\"month\":10,\"year\":2012,\"count\":186,\"metric\":\"ALL\",\"value\":\"\"}";

		assertEquals(expectedOutput, Files.toString(new File(OUTPUT, "part-m-00000"), Charset.defaultCharset()).trim());
		
		HadoopUtils.deleteIfExists(FileSystem.get(getConf()), new Path(OUTPUT));
	}
}
