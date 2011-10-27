package org.apache.solr.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.solr.hadoop.SolrRecordWriter;

import java.io.IOException;

public class CSVReducer extends Reducer<LongWritable, Text, LongWritable, Text> {


  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    SolrRecordWriter.addReducerContext(context);
  }
}
