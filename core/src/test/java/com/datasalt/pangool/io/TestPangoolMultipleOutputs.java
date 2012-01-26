package com.datasalt.pangool.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Assert;
import org.junit.Test;

public class TestPangoolMultipleOutputs {

	@Test
	public void testSpecificContext() throws IOException {
		// Test that we can add specific key, value configurations for each output
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		PangoolMultipleOutputs.addNamedOutput(job, "foo", TextOutputFormat.class, Text.class, Text.class);
		PangoolMultipleOutputs.addNamedOutputContext(job, "foo", "my.context.property", "myValue");

		PangoolMultipleOutputs.setSpecificNamedOutputContext(job.getConfiguration(), job, "foo");
		Assert.assertEquals("myValue", job.getConfiguration().get("my.context.property"));
	}
}
