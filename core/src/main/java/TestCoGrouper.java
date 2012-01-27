import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Field.Order;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;
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
import com.datasalt.avrool.SerializationInfo;
import com.datasalt.avrool.api.GroupHandler;
import com.datasalt.avrool.api.InputProcessor;
import com.datasalt.avrool.commons.HadoopUtils;




public class TestCoGrouper {

	//public static final String NAMESPACE = "com.datasalt";
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
	    Schema schema = conf.getSchemaBySource("usuarios");
			Record r = new Record(schema);
			Random random = new Random();
			r.put("user_id",3);
			r.put("name",random.nextInt() + "");
			r.put("age",random.nextInt());
			r.put("my_bytes",new byte[]{12,3,21});
	    collector.write(r);
    }
		
	}
	
	
	public static final class MyGroupHandler extends GroupHandler<Text,Text> {
		public void onGroupElements(GenericRecord group, Iterable<GenericRecord> records, 
				CoGrouperContext<Text, Text> pangoolContext, Collector<Text, Text> collector) throws IOException, InterruptedException,
    CoGrouperException {

			System.out.println(group);
			for (GenericRecord r : records){
				String i = Integer.toHexString(System.identityHashCode(r));

				System.out.println(r.getClass() +" "+ i  + " " + r);
			}
			
			
			
		}
		
		
	}
	
	
	public static void main(String[] args) throws CoGrouperException, IOException, InterruptedException, ClassNotFoundException{
		
		CoGrouperConfigBuilder b = CoGrouperConfigBuilder.newOne();
		
		List<Field> userFields = new ArrayList<Field>();
		userFields.add(new Field("user_id", Schema.create(Type.INT),null,null));
		userFields.add(new Field("name", Schema.create(Type.STRING),null,null));
		userFields.add(new Field("age", Schema.create(Type.INT),null,null));
		userFields.add(new Field("my_bytes", Schema.create(Type.BYTES),null,null));
			
		List<Field> countryFields = new ArrayList<Field>();
		countryFields.add(new Field("user_id", Schema.create(Type.INT),null,null));
		countryFields.add(new Field("name", Schema.create(Type.STRING),null,null));
		countryFields.add(new Field("country", Schema.create(Type.STRING),null,null));
		countryFields.add(new Field("num_people", Schema.create(Type.INT),null,null));
		countryFields.add(new Field("another", Schema.create(Type.BYTES),null,null));
				
		Schema usersSchema = Schema.createRecord("usuarios", null, NAMESPACE, false);
		usersSchema.setFields(userFields);

		Schema countriesSchema = Schema.createRecord("countries", null, NAMESPACE, false);
		countriesSchema.setFields(countryFields);

		b.addSource(usersSchema);
		b.addSource(countriesSchema);
		b.setGroupByFields("user_id");
		b.setCommonOrdering(new Ordering().add("user_id",Order.DESCENDING).add("name",Order.DESCENDING));
		b.setInterSourcesOrdering(Order.DESCENDING);
		
		b.setIndividualSourceOrdering(usersSchema.getFullName(), new Ordering().add("age",Order.DESCENDING).add("my_bytes",Order.DESCENDING));
		b.setIndividualSourceOrdering(countriesSchema.getFullName(),new Ordering().add("country", Order.DESCENDING));
		
		CoGrouperConfig config = b.build();
		
		System.out.println(config.getSchemasBySource());
		
		SerializationInfo serInfo = SerializationInfo.get(config);
		
		
		
		System.out.println("Common : " + serInfo.getCommonSchema());
		System.out.println("Particular : " + serInfo.getParticularSchemas());
		System.out.println("Intermediate: " + serInfo.getIntermediateSchema());
		
		System.out.println("Intermediat2: " + Schema.parse(serInfo.getIntermediateSchema().toString()));
		
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
