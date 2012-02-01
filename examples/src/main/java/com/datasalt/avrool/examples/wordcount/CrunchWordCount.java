package com.datasalt.avrool.examples.wordcount;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.lib.Aggregate;
import com.cloudera.crunch.type.writable.Writables;

public class CrunchWordCount {
  public static void main(String[] args) throws Exception {
    Pipeline pipeline = new MRPipeline(CrunchWordCount.class);    
    PCollection<String> lines = pipeline.readTextFile(args[0]);

    PCollection<String> words = lines.parallelDo("my splitter", new DoFn<String, String>() {
      public void process(String line, Emitter<String> emitter) {
        for (String word : line.split("\\s+")) {
          emitter.emit(word);
        }
      }
    }, Writables.strings());

    PTable<String, Long> counts = Aggregate.count(words);

    pipeline.writeTextFile(counts, args[1]);
    pipeline.run();
    
  }
}
