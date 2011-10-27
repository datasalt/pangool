/**
 * 
 */
package org.apache.solr.hadoop;

import java.io.IOException;
import java.io.StringReader;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVStrategy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CSVMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
  private static final Log LOG = LogFactory.getLog(CSVMapper.class);
  
  private Text id = new Text();
  private String prefix = null;
  private boolean skipHeader = false;
  private String[] fieldNames = null;
  private CSVStrategy strategy = new CSVStrategy(',', '"',
          CSVStrategy.COMMENTS_DISABLED, '\\', false, false, true, true);

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    prefix = context.getConfiguration().get("mapred.task.partition") + "-";
    if (context.getConfiguration().getLong("map.input.start", -1) == 0) {
      skipHeader = true;
    }
    String file = ((FileSplit) context.getInputSplit()).getPath().toUri().getPath();
//    if (file == null) {
//      LOG.warn("no input file name");
//      return;
//    }
    LOG.debug("getting headers for: " + file);
    fieldNames = context.getConfiguration().getStrings(CSVIndexer.FIELD_NAMES_KEY + file);
  }

  MapWritable res = new MapWritable();
  
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    id.set(prefix + key.toString());
    if (key.get() == 0 && skipHeader) { // this is the CSV header line
      return;
    }
    CSVParser parser = new CSVParser(new StringReader(value.toString()), strategy);
    String[] fields = null;
    try {
      fields = parser.getLine();
    } catch (Exception e) {
      LOG.warn("invalid line: '" + value + "': " + e.toString());
      return;
    }
    if (fieldNames != null && fields.length != fieldNames.length) {
      LOG.warn("header.len != line.len: " + value.toString());
      return;
    }
    res.clear();
    for (int i = 0; i < fields.length; i++) {
      if (fieldNames != null) {
        res.put(new Text(fieldNames[i]), new Text(fields[i]));
      } else {
        res.put(new Text("f" + i), new Text(fields[i]));
      }
    }
    context.write(id, res);
  }
}
