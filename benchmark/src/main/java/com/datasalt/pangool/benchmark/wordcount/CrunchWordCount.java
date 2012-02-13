package com.datasalt.pangool.benchmark.wordcount;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.lib.Aggregate;
import com.cloudera.crunch.type.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.Serializable;

/**
 * Code for solving the simple PangoolWordCount problem in Crunch.
 */
@SuppressWarnings("serial")
public class CrunchWordCount extends Configured implements Tool, Serializable {

  public int run(String[] args) throws Exception {
    if(args.length != 2) {
      System.err.println();
      System.err.println("Two and only two arguments are accepted.");
      System.err.println("Usage: " + this.getClass().getName() + " [generic options] input output");
      System.err.println();
      GenericOptionsParser.printGenericCommandUsage(System.err);
      return 1;
    }
    // Create an object to coordinate pipeline creation and execution.
    Pipeline pipeline = new MRPipeline(PangoolWordCount.class, getConf());
    // Reference a given text file as a collection of Strings.
    PCollection<String> lines = pipeline.readTextFile(args[0]);

    // Define a function that splits each line in a PCollection of Strings into a
    // PCollection made up of the individual words in the file.
    PCollection<String> words = lines.parallelDo(new DoFn<String, String>() {
      public void process(String line, Emitter<String> emitter) {
        for (String word : line.split("\\s+")) {
          emitter.emit(word);
        }
      }
    }, Writables.strings()); // Indicates the serialization format

    // The Aggregate.count method applies a series of Crunch primitives and returns
    // a map of the unique words in the input PCollection to their counts.
    // Best of all, the count() function doesn't need to know anything about
    // the kind of data stored in the input PCollection.
    PTable<String, Long> counts = Aggregate.count(words);

    // Instruct the pipeline to write the resulting counts to a text file.
    pipeline.writeTextFile(counts, args[1]);
    // Execute the pipeline as a MapReduce.
    pipeline.done();
    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new CrunchWordCount(), args);
  }
}
