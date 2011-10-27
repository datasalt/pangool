package org.apache.solr.hadoop;


import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.solr.hadoop.SolrDocumentConverter;
import org.apache.solr.hadoop.SolrOutputFormat;

public class CSVIndexer extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(CSVIndexer.class);
  
  public static final String FIELD_NAMES_KEY = "csv.names.";

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: CSVIndexer <outputDir> -solr <solrHome> <inputDir> [<inputDir2> ...] [-shards NNN] [-compress_output]");
      System.err.println("\tinputDir\tinput directory(-ies) containing CSV files");
      System.err.println("\toutputDir\toutput directory containing Solr indexes.");
      System.err.println("\tsolr <solrHome>\tlocal directory containing Solr conf/ and lib/");
      System.err.println("\tshards NNN\tset the number of output shards to NNN");
      System.err.println("\t\t(default: the default number of reduce tasks)");
      System.err.println("\tcompress_output\tto compress the output of the reducer tasks (create .zip file)");
      return -1;
    }
    Job job = new Job(getConf());
    job.setJarByClass(CSVIndexer.class);

    int shards = -1;
    boolean compressOutput = false;
    String solrHome = null;
    Path out = new Path(args[0]);
    for (int i = 1; i < args.length; i++) {
      if (args[i] == null) continue;
      if (args[i].equals("-shards")) {
        shards = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-compress_output")) {
        compressOutput = true;
      } else if (args[i].equals("-solr")) {
        solrHome = args[++i];
        continue;
      } else {
        Path p = new Path(args[i]);
        FileInputFormat.addInputPath(job, p);
        FileSystem fs = p.getFileSystem(job.getConfiguration());
        for (FileStatus stat : fs.listStatus(p)) {
          if (stat.isDir()) {
            continue;
          }
          getHeader(fs, stat, job);
        }
      }
    }
    if (solrHome == null || !new File(solrHome).exists()) {
      throw new IOException("You must specify a valid solr.home directory!");
    }
    job.setMapperClass(CSVMapper.class);
    job.setReducerClass(CSVReducer.class);
    job.setOutputFormatClass(SolrOutputFormat.class);
    SolrOutputFormat.setupSolrHomeCache(new File(solrHome), job.getConfiguration());
    if (shards > 0) {
      job.setNumReduceTasks(shards);
    }
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MapWritable.class);
    SolrDocumentConverter.setSolrDocumentConverter(CSVDocumentConverter.class, job.getConfiguration());
    FileOutputFormat.setOutputPath(job, out);
    SolrOutputFormat.setOutputZipFormat(compressOutput, job.getConfiguration());

    return job.waitForCompletion(true) ? 0 : -1;
  }
  
  private void getHeader(FileSystem fs, FileStatus s, Job job) throws IOException {
    InputStream is = fs.open(s.getPath());
    BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
    String line = br.readLine();
    br.close();
    String[] fields = line.split(",");
    LOG.debug("storing header for: " + s.getPath().toUri().getPath());
    job.getConfiguration().setStrings(FIELD_NAMES_KEY + s.getPath().toUri().getPath(), fields);
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new CSVIndexer(), args);
    System.exit(res);
  }

}
