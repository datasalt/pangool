import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.datasalt.pangool.CoGrouper;
import com.datasalt.pangool.CoGrouperConfig;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.Criteria.Order;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Schema.Field;
import com.datasalt.pangool.SortBy;
import com.datasalt.pangool.api.GroupHandler;
import com.datasalt.pangool.api.GroupHandlerWithRollup;
import com.datasalt.pangool.api.InputProcessor;
import com.datasalt.pangool.api.GroupHandler.CoGrouperContext;
import com.datasalt.pangool.api.GroupHandler.Collector;
import com.datasalt.pangool.commons.HadoopUtils;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Tuple;

public class TestMonoSourceRollUp {

	public static class MyInputProcessor extends InputProcessor<LongWritable, Text>{

    private static final long serialVersionUID = 1L;
		private CoGrouperConfig conf;
		
		/**
		 * Called once at the start of the task. Override it to implement your custom logic.
		 */
		public void setup(CoGrouperContext context, Collector collector) throws IOException, InterruptedException {
			this.conf = context.getCoGrouperConfig();
		}
		
		@Override
    public void process(LongWritable key, Text value, CoGrouperContext context,
        InputProcessor.Collector collector) throws IOException, InterruptedException {
	    Schema usuariosSchema = conf.getSourceSchema("usuarios");
	   Random random = new Random();
			
			Tuple userRecord = new Tuple(usuariosSchema);
			userRecord.set("user_id",3);
			userRecord.set("name",(random.nextBoolean() ? "blabla" : (random.nextInt() +"")));
			userRecord.set("age",random.nextFloat());
			
	    collector.write(userRecord);
	    
	    userRecord.set("user_id",5);
			userRecord.set("name",(random.nextBoolean() ? "blabla" : (random.nextInt() +"")));
			userRecord.set("age",random.nextFloat());
	    collector.write(userRecord);
	   
    }
		
	}
	
	public static final class MyGroupHandler extends GroupHandlerWithRollup<Text, Text> {
		public void onGroupElements(ITuple group, Iterable<ITuple> tuples,
				CoGrouperContext coGrouperContext, Collector collector)
				throws IOException, InterruptedException, CoGrouperException {
			System.out.println("Group : " + group);
			for (ITuple r : tuples) {
				String i = Integer.toHexString(System.identityHashCode(r));
				System.out.println(r + " => " + r.getSchema().getName());
			}
		}
		
		public void onOpenGroup(int depth, String field, ITuple firstElement, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException, CoGrouperException {
			System.out.println("Open group " + depth + " " + field);
		}
		
		public void onCloseGroup(int depth, String field, ITuple lastElement,
		    CoGrouperContext context, Collector collector) throws IOException, InterruptedException, CoGrouperException {
			System.out.println("Close group " + depth + " " + field);
		}
	}
	
	public static void main(String[] args) throws CoGrouperException, IOException, InterruptedException, ClassNotFoundException{
		
		List<Field> userFields = new ArrayList<Field>();
		userFields.add(new Field("name", String.class));
		userFields.add(new Field("user_id", Integer.class));
		userFields.add(new Field("age", Float.class));
		Schema usersSchema = new Schema("usuarios",userFields);

		System.out.println("users Schema : " + usersSchema.getName() + " "+ usersSchema);
		
		Path outputPath = new Path("pangool_output");
		CoGrouper coGrouper = new CoGrouper(new Configuration());
		coGrouper.addSourceSchema(usersSchema);
		coGrouper.setGroupByFields("user_id","name");
		coGrouper.setRollupFrom("user_id");
		
		coGrouper.setOrderBy(new SortBy().add("user_id",Order.ASC).add("name",Order.ASC).addSourceOrder(Order.ASC));
		//coGrouper.setSecondaryOrderBy("usuarios", new Criteria().add("age",Order.ASC));
		
		
		coGrouper.addInput(new Path("pangool_input.txt"), TextInputFormat.class, new MyInputProcessor());
		coGrouper.setGroupHandler(new MyGroupHandler());
		coGrouper.setOutput(outputPath, TextOutputFormat.class, Text.class, Text.class);
		Job job = coGrouper.createJob();
		
		HadoopUtils.deleteIfExists(FileSystem.get(job.getConfiguration()),outputPath);
		job.waitForCompletion(true);
	}
}
