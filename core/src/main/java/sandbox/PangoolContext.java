package sandbox;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import com.datasalt.pangolin.grouper.io.tuple.DoubleBufferPangolinTuple;

public class PangoolContext extends Mapper.Context {

	public PangoolContext(Mapper mapper, Configuration arg0, TaskAttemptID arg1, RecordReader arg2, RecordWriter arg3,
      OutputCommitter arg4, StatusReporter arg5, InputSplit arg6) throws IOException, InterruptedException {
	  mapper.super(arg0, arg1, arg2, arg3, arg4, arg5, arg6);
  }

	public void write(DoubleBufferPangolinTuple tuple) throws IOException,InterruptedException {
		write(tuple, NullWritable.get());
	}
}
