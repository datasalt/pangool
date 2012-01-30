
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.BinaryData;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroKeyComparator;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.datasalt.avrool.commons.HadoopUtils;
import com.datasalt.avrool.mapreduce.PangoolGroupComparator;

public class AvroTest {

	public static final String NAMESPACE = "com.datasalt";

	private static final Schema union1 = Schema.createRecord("union1", null, NAMESPACE, false);
	private static final Schema union2 = Schema.createRecord("union2", null, NAMESPACE, false);
	static {
		union1.setFields(Arrays.asList(new Field("my_animal", Schema.create(Type.INT), null, null,
		    Field.Order.ASCENDING)));
		union2
		    .setFields(Arrays.asList(new Field("my_animal", Schema.create(Type.INT), null, null, Field.Order.ASCENDING)));
	}

	
	public static class Mapy extends Mapper<LongWritable, Text, AvroKey<Record>, AvroValue> {

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Schema pairSchema = AvroJob.getMapOutputSchema(context.getConfiguration());
			Schema keySchema = Pair.getKeySchema(pairSchema);

			String[] tokens = value.toString().split("\\s+");
			Integer userId = Integer.parseInt(tokens[0]);
			String name = tokens[1];
			Integer age = Integer.parseInt(tokens[2]);

			Record recordKey = new Record(keySchema);
			recordKey.put("user_id", userId);
			recordKey.put("name", name);
			recordKey.put("age", age);
			recordKey.put("my_bytes", new byte[]{23,23,25});

			List<Integer> myArray = new ArrayList<Integer>();
			myArray.add(new Random().nextInt());
			recordKey.put("my_array", myArray);
			//recordKey.put("my_map", new HashMap());
			//recordKey.put("my_enum", SortOrder.ASCENDING.toString());

			for(int i = 0; i < 10; i++) {
				if(new Random().nextBoolean()) {

					Record unionObject = new Record(union1);
					unionObject.put("my_animal", new Random().nextInt());
					recordKey.put("my_union", unionObject);
				} else {
					Record unionObject = new Record(union2);
					unionObject.put("my_animal", new Random().nextInt());
					recordKey.put("my_union", unionObject);
				}

				context.write(new AvroKey(recordKey), new AvroValue(null));
			}

		};

	}

	public static class Red extends Reducer<AvroKey<Record>, AvroValue, Text, Text> {

		@Override
		protected void reduce(AvroKey<Record> key, Iterable<AvroValue> values, Context context) throws IOException,
		    InterruptedException {
			context.write(new Text("Reduce"), new Text(""));
			for(AvroValue value : values) {
				Record record = key.datum();
				Record unionRecord = (Record) record.get("my_union");
				context.write(new Text(record.toString()), new Text(unionRecord.getSchema().getFullName()));
			}

		};

	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

//		printSchemaTest();
//		System.exit(0);
		
		
		List<Field> fields = new ArrayList<Field>();
		fields.add(new Field("user_id", Schema.create(Type.INT), null, null, Field.Order.DESCENDING));
		fields.add(new Field("name", Schema.create(Type.STRING), null, null, Field.Order.DESCENDING));
		fields.add(new Field("age", Schema.create(Type.INT), null, null, Field.Order.DESCENDING));
		fields.add(new Field("my_bytes", Schema.create(Type.BYTES), null, null, Field.Order.DESCENDING));
		
		
		
		
//		List<Field> fields = new ArrayList<Field>();
//		fields.add(new Field("user_id", PangoolSchema.create(Type.INT), null, null));
//		fields.add(new Field("name", PangoolSchema.create(Type.STRING), null, null));
//		fields.add(new Field("age", PangoolSchema.create(Type.INT), null, null, Field.Order.DESCENDING));
//		
		
		// fields.add(new
		// Field("otracosa",PangoolSchema.createFixed("otracosa",null,null,12),null,null,Field.Order.DESCENDING));
		fields.add(new Field("my_array", Schema.createArray(Schema.create(Type.INT)), null, null, Field.Order.DESCENDING));
		//fields.add(new Field("my_map", PangoolSchema.createMap(PangoolSchema.create(Type.STRING)), null, null, Field.Order.DESCENDING));

		List<Schema> unionSchemas = new ArrayList<Schema>();
		unionSchemas.add(union1);
		unionSchemas.add(union2);
		Schema unionSchema = Schema.createUnion(unionSchemas);

		fields.add(new Field("my_union", unionSchema, null, null, Field.Order.ASCENDING));

//		List<String> enumFields = new ArrayList<String>();
//
//		enumFields.add(SortOrder.ASCENDING.toString());
//		enumFields.add(SortOrder.DESCENDING.toString());

//		fields.add(new Field("my_enum", PangoolSchema.createEnum("my_enum", null, NAMESPACE, enumFields), null, null,
//		    Field.Order.DESCENDING));

		Schema intermediateSchema = Schema.createRecord("my_schema", null, NAMESPACE, false);
		//PangoolSchema intermediateSchema = PangoolSchema.createRecord(fields);
		intermediateSchema.setFields(fields);
		
		fields = new ArrayList<Field>();
		fields.add(new Field("user_id", Schema.create(Type.INT), null, null, Field.Order.DESCENDING));

		Schema groupSchema = Schema.createRecord("group_schema", null, NAMESPACE, false);
		groupSchema.setFields(fields);

		JobConf conf = new JobConf();
		Schema nullSchema = Schema.create(Schema.Type.NULL);
		Schema pairSchema = Pair.getPairSchema(intermediateSchema, nullSchema);
		AvroJob.setMapOutputSchema(conf, pairSchema);
		
		//conf.set(PangoolGroupComparator.CONF_GROUP_SCHEMA, groupSchema.toString());

		Job job = new Job(conf);
		job.setMapperClass(Mapy.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(Red.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(AvroKey.class);
		job.setMapOutputValueClass(AvroValue.class);
		job.setGroupingComparatorClass(PangoolGroupComparator.class);
		job.setPartitionerClass(Part.class);
		
		FileInputFormat.addInputPath(job, new Path("avro_input.txt"));
		Path outputPath = new Path("avro_output.txt");
		FileOutputFormat.setOutputPath(job, outputPath);

		HadoopUtils.deleteIfExists(FileSystem.get(conf), outputPath);
		job.waitForCompletion(true);

	}
	
	
	public static class Part extends Partitioner<AvroKey<Record>,AvroValue> {

		@Override
    public int getPartition(AvroKey<Record> key, AvroValue value, int numReducers) {
	    // TODO Auto-generated method stub
	    return 0;
    }

		

		
		
	}
	
	private static void printSchemaTest(){
		
		
		List<Field> usuariosFields = new ArrayList<Field>();
		usuariosFields.add(new Field("name", Schema.create(Type.STRING), null, null, Field.Order.DESCENDING));
		usuariosFields.add(new Field("mis_notas", Schema.createArray(Schema.create(Type.INT)), null, null, Field.Order.DESCENDING));
		usuariosFields.add(new Field("otra_informacion", Schema.createArray(Schema.create(Type.INT)), null, null, Field.Order.DESCENDING));
		
		
		List<Field> countryFields = new ArrayList<Field>();
		countryFields.add(new Field("country", Schema.create(Type.STRING), null, null, Field.Order.DESCENDING));
		
		
		
		Schema usuariosSchema = Schema.createRecord("usuarios",null,NAMESPACE,false);
		usuariosSchema.setFields(usuariosFields);
		Schema countrySchema = Schema.createRecord("user_countries",null,NAMESPACE,false);
		countrySchema.setFields(countryFields);
		
		List<Schema> unionSchemas = new ArrayList<Schema>();
		unionSchemas.add(usuariosSchema);
		unionSchemas.add(countrySchema);
		Schema unionSchema = Schema.createUnion(unionSchemas);
		
		List<Field> fields = new ArrayList<Field>();
		fields.add(new Field("user_id", Schema.create(Type.INT), null, null));
		fields.add(new Field("mi_union",unionSchema,null,null,Field.Order.DESCENDING));
		
		Schema schemaFinal = Schema.createRecord(fields);
		
		System.out.println(schemaFinal);
		
		
	}
	

}
