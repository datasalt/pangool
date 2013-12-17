package com.datasalt.pangool.examples.mongo;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.datasalt.pangool.examples.BaseExampleJob;
import com.datasalt.pangool.tuplemr.MapOnlyJobBuilder;
import com.datasalt.pangool.tuplemr.MultipleOutputsCollector;
import com.datasalt.pangool.tuplemr.mapred.MapOnlyMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.HadoopOutputFormat;
import com.google.common.io.Files;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

/**
 * This simple example shows how to use mongodb-hadoop with Pangool. In this example we process HTML files
 * from Qype and extract the reviews from them, inserting the reviews into Mongodb.
 */
@SuppressWarnings("serial")
public class QypeScrapper extends BaseExampleJob implements Serializable {

	String outPath = "tmp-path";

	public QypeScrapper() {
		super("Usage: [config_file (for PARSING)] [input_path (with HTML QYPE files)]");
	}

	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2) {
			failArguments("Invalid number of arguments");
			return -1;
		}
		String parsingFile = args[0];
		String inputFolder = args[1];

		// The following lines read "parsing configuration" for poor-man's parsing the reviews of Qype website.
		// Every line is a property of the review and the associated Regex.
		final Map<String, Pattern> parsingConfig = new HashMap<String, Pattern>();
		for(String line : Files.readLines(new File(parsingFile), Charset.defaultCharset())) {
			parsingConfig.put(line.split("\t")[0], Pattern.compile(line.split("\t")[1]));
		}
		final Pattern startPattern = parsingConfig.get("start"); // special pattern: start of review
		final Pattern endPattern = parsingConfig.get("end"); // special pattern: end of review
		// retain only review-local patterns
		parsingConfig.remove("start");
		parsingConfig.remove("end");

		// The pattern for getting the place Id
		final Pattern placePattern = Pattern.compile("\\Q\"/place/\\E([\\d]*)");
		
		delete(outPath);

		// We only need to execute a Map-only job for this task.
		// Every map will process a HTML file and extract the reviews from it.
		MapOnlyJobBuilder builder = new MapOnlyJobBuilder(conf);

		builder.addInput(new Path(inputFolder), new HadoopInputFormat(TextInputFormat.class),
		    new MapOnlyMapper<LongWritable, Text, Text, BSONObject>() {

			    StringBuffer inMemoryHtml = new StringBuffer();

			    @Override
			    protected void map(LongWritable key, Text value, Context context) throws IOException,
			        InterruptedException {
			    	// for every line in the HTML just add it to a string buffer
			    	// we will process the entire HTML in the end (cleanup())
				    inMemoryHtml.append(value.toString());
			    }

			    @Override
			    protected void cleanup(Context context, MultipleOutputsCollector coll) throws IOException, InterruptedException {
				    String html = inMemoryHtml.toString();

				    Matcher startMatcher = startPattern.matcher(html);
				    Matcher endMatcher = endPattern.matcher(html);

				    Text documentId = new Text();
				    
				    Matcher placeMatcher = placePattern.matcher(html);
				    // we assume this will always match - otherwise fail fast!
				    placeMatcher.find();
				    String placeId = placeMatcher.group(1);

				    // Now we will proceed as follows:
				    // We create a regex matcher for start of reviews and end of reviews
				    // Within each (start, end) pair, we will execute an arbitrary number of matchers
				    // for matching all the other properties (username, date, rating, review text...).
				    // finally we add all the properties to a Mongo BSONObject that can be used as output.
				    while(startMatcher.find()) {
					    BSONObject review = new BasicBSONObject();
					    review.put("place_id", placeId);
					    int reviewStart = startMatcher.start();
					    endMatcher.find();
					    int reviewEnd = endMatcher.start();

					    // Focus only on (start, end) text for this review
					    String reviewText = html.substring(reviewStart, reviewEnd);
					    
					    for(Map.Entry<String, Pattern> parsingProperty : parsingConfig.entrySet()) {
						    Matcher matcher = parsingProperty.getValue().matcher(reviewText);
						    if(matcher.find()) {
						    	review.put(parsingProperty.getKey(), matcher.group(1).trim());
						    }
					    }
					    
					    // The Mongo documentId of the review will the be the  Review_id.
					    documentId.set((String) review.get("review_id"));
					    // Write the pair (Id, document) to the output collector.
					    context.write(documentId, review);
				    }
			    }
		    });

		// --- This is the most important part (what makes it work with MongoDB: ---
		// Set the URL of the MongoDB we will write to. Here we specify the DB and the final Table.
		MongoConfigUtil.setOutputURI(conf, "mongodb://localhost/test.qype");
		// Set the output format to HadoopOutputFormat(MongoOutputFormat.class)
		// The key will be the documentIds for the Mongo table and the value a Mongo BSONObject with all the properties we wish.
		builder.setOutput(new Path(outPath), new HadoopOutputFormat(MongoOutputFormat.class), Text.class,
		    BSONObject.class);

		// Finally, build and execute the Pangool Job.
		try {
			builder.createJob().waitForCompletion(true);
		} finally {
			builder.cleanUpInstanceFiles();
		}
		
		// we are not interested in the output folder, so delete it
		delete(outPath);
		
		return 1;
	}

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new QypeScrapper(), args);
	}

}
