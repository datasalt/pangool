import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Field.Order;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.datasalt.avrool.CoGrouper;
import com.datasalt.avrool.CoGrouperConfig;
import com.datasalt.avrool.CoGrouperConfigBuilder;
import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.Ordering;
import com.datasalt.avrool.api.GroupHandler;
import com.datasalt.avrool.api.InputProcessor;
import com.datasalt.avrool.commons.HadoopUtils;




public class TestCoGrouper {

	public static final String NAMESPACE = null;
	
	public static class MyInputProcessor extends InputProcessor<LongWritable, Text>{

		private CoGrouperConfig conf;
		
		/**
		 * Called once at the start of the task. Override it to implement your custom logic.
		 */
		public void setup(CoGrouperContext context, Collector collector) throws IOException, InterruptedException {
			this.conf = context.getPangoolConfig();
		}
		
		@Override
    public void process(LongWritable key, Text value, com.datasalt.avrool.api.InputProcessor.CoGrouperContext context,
        com.datasalt.avrool.api.InputProcessor.Collector collector) throws IOException, InterruptedException {
	    Schema usuariosSchema = conf.getSchemaBySource("usuarios");
	    Schema countriesSchema = conf.getSchemaBySource("countries");
	    Random random = new Random();
			
	    
	    //Serialization ser = new Serialization(conf);
	    
	     //ByteBuffer.
	    
			Record userRecord = new Record(usuariosSchema);
			userRecord.put("user_id",3);
			userRecord.put("name",(random.nextBoolean() ? "blabla" : (random.nextInt() +"")));
			userRecord.put("age",random.nextInt());
			userRecord.put("my_bytes",ByteBuffer.wrap(new byte[]{12,3,21}));
	    collector.write(userRecord);
	    
	    userRecord.put("user_id",5);
			userRecord.put("name",(random.nextBoolean() ? "blabla" : (random.nextInt() +"")));
			userRecord.put("age",random.nextInt());
			userRecord.put("my_bytes",ByteBuffer.wrap(new byte[]{12,3,21}));
	    collector.write(userRecord);
	    
	   
	    Record countryRecord = new Record(countriesSchema);
			countryRecord.put("user_id",3);
			countryRecord.put("name",(random.nextBoolean() ? "blabla" : (random.nextInt() +"")));
			countryRecord.put("country",Integer.toString(random.nextInt()));
			countryRecord.put("another",ByteBuffer.wrap(new byte[]{12,3,21},0,1));
			countryRecord.put("num_people",random.nextInt());
	    collector.write(countryRecord);
	    
	    countryRecord.put("user_id",5);
			countryRecord.put("name",(random.nextBoolean() ? "blabla" : (random.nextInt() +"")));
			countryRecord.put("country",Integer.toString(random.nextInt()));
			countryRecord.put("another",ByteBuffer.wrap(new byte[]{12,3,21}));
			countryRecord.put("num_people",random.nextInt());
	    collector.write(countryRecord);
	   
    }
		
	}
	
	
	public static final class MyGroupHandler extends GroupHandler<Text,Text> {
		public void onGroupElements(GenericRecord group, Iterable<GenericRecord> records, 
				CoGrouperContext<Text, Text> pangoolContext, Collector<Text, Text> collector) throws IOException, InterruptedException,
    CoGrouperException {

			System.out.println("Group : " + group);
			for (GenericRecord r : records){
				String i = Integer.toHexString(System.identityHashCode(r));
				System.out.println(r + " => " + r.getSchema().getFullName());
			}
		}
	}
	
	
	public static void main(String[] args) throws CoGrouperException, IOException, InterruptedException, ClassNotFoundException{
		
		CoGrouperConfigBuilder b = CoGrouperConfigBuilder.newOne();
		
		List<Field> userFields = new ArrayList<Field>();
		
		userFields.add(new Field("name", Schema.create(Type.STRING),null,null));
		userFields.add(new Field("user_id", Schema.create(Type.INT),null,null));
		userFields.add(new Field("age", Schema.create(Type.INT),null,null));
		userFields.add(new Field("my_bytes", Schema.create(Type.BYTES),null,null));
			
		List<Field> countryFields = new ArrayList<Field>();
		countryFields.add(new Field("user_id", Schema.create(Type.INT),null,null));
		countryFields.add(new Field("name", Schema.create(Type.STRING),null,null));
		countryFields.add(new Field("num_people", Schema.create(Type.INT),null,null));
		countryFields.add(new Field("country", Schema.create(Type.STRING),null,null));
		countryFields.add(new Field("another", Schema.create(Type.BYTES),null,null));

		Schema usersSchema = Schema.createRecord("usuarios", null, NAMESPACE, false);
		usersSchema.setFields(userFields);

		Schema countriesSchema = Schema.createRecord("countries", null, NAMESPACE, false);
		countriesSchema.setFields(countryFields);

		b.addSource(usersSchema);
		b.addSource(countriesSchema);
		b.setGroupByFields("user_id");
		b.setCommonOrdering(new Ordering().add("user_id",Order.DESCENDING).add("name",Order.ASCENDING));
		b.setInterSourcesOrdering(Order.DESCENDING);
		
		b.setIndividualSourceOrdering(usersSchema.getFullName(), new Ordering().add("age",Order.DESCENDING).add("my_bytes",Order.DESCENDING));
		b.setIndividualSourceOrdering(countriesSchema.getFullName(),new Ordering().add("country", Order.DESCENDING));
		
		CoGrouperConfig config = b.build();

		System.out.println(config.getSchemasBySource());
		
		Path outputPath = new Path("avrool_output");
		CoGrouper coGrouper = new CoGrouper(config,new Configuration());
		coGrouper.addInput(new Path("avrool_input.txt"), TextInputFormat.class, MyInputProcessor.class);
		coGrouper.setGroupHandler(MyGroupHandler.class);
		coGrouper.setOutput(outputPath, TextOutputFormat.class, Text.class, Text.class);
		Job job = coGrouper.createJob();
		
		HadoopUtils.deleteIfExists(FileSystem.get(job.getConfiguration()),outputPath);
		job.waitForCompletion(true);
		
	}
	
}
