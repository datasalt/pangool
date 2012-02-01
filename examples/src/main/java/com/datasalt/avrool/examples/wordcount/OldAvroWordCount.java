package com.datasalt.avrool.examples.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.AvroTextOutputFormat;
import org.apache.avro.mapred.AvroUtf8InputFormat;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

public class OldAvroWordCount {

  public static class MapImpl extends AvroMapper<Utf8, Pair<Utf8, Long> > {
    @Override
      public void map(Utf8 text, AvroCollector<Pair<Utf8,Long>> collector,
                      Reporter reporter) throws IOException {
      StringTokenizer tokens = new StringTokenizer(text.toString());
      while (tokens.hasMoreTokens())
        collector.collect(new Pair<Utf8,Long>(new Utf8(tokens.nextToken()),1L));
    }
  }
  
  public static class ReduceImpl
    extends AvroReducer<Utf8, Long, Pair<Utf8, Long> > {
    @Override
    public void reduce(Utf8 word, Iterable<Long> counts,
                       AvroCollector<Pair<Utf8,Long>> collector,
                       Reporter reporter) throws IOException {
      long sum = 0;
      for (long count : counts)
        sum += count;
      collector.collect(new Pair<Utf8,Long>(word, sum));
    }
  }    

  

  @SuppressWarnings("deprecation")
  public JobConf getJob(String input,String output) throws Exception {
    JobConf job = new JobConf();
    Path outputPath = new Path(output);
    
    outputPath.getFileSystem(job).delete(outputPath);
    
    job.setJarByClass(OldAvroWordCount.class);
    job.setJobName("old avro wordcount");
    job.setInputFormat(AvroUtf8InputFormat.class);
    AvroJob.setInputSchema(job, Schema.create(Schema.Type.STRING));
    AvroJob.setOutputSchema(job,
                            new Pair<Utf8,Long>(new Utf8(""), 0L).getSchema());
    
    AvroJob.setMapperClass(job, MapImpl.class);        
    AvroJob.setCombinerClass(job, ReduceImpl.class);
    AvroJob.setReducerClass(job, ReduceImpl.class);
    
    FileInputFormat.setInputPaths(job, new Path(input));
    FileOutputFormat.setOutputPath(job, outputPath);
    //FileOutputFormat.setCompressOutput(job, true);
    
    //WordCountUtil.setMeta(job);
    return job;
   // JobClient.runJob(job);
    
    //WordCountUtil.validateCountsFile();
  }
  
  
  public static void main(String args[]) throws Exception {
  	if(args.length != 2) {
  		System.err.println("Wrong number of arguments");
  		System.err.println("input output");
  		System.exit(-1);
  	}

  	
  	JobConf job = new OldAvroWordCount().getJob(args[0], args[1]);
  	JobClient.runJob(job);
  }
}