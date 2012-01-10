package sandbox;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.junit.Test;

import com.datasalt.pangolin.commons.HadoopUtils;
import com.datasalt.pangolin.commons.test.AbstractHadoopTestLibrary;
import com.datasalt.pangolin.grouper.Grouper;
import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.Schema;
import com.datasalt.pangolin.grouper.SortCriteria;
import com.datasalt.pangolin.grouper.io.tuple.ITuple;
import com.datasalt.pangolin.grouper.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangolin.grouper.io.tuple.Tuple;
import com.datasalt.pangolin.grouper.io.tuple.TupleFactory;
import com.datasalt.pangolin.grouper.mapreduce.InputProcessor;
import com.datasalt.pangolin.grouper.mapreduce.handler.GroupHandler;
import com.datasalt.pangolin.thrift.test.TestEntity;

/**
 * Simple example that uses a Thrift entity 
 * 
 * @author pere
 *
 */
public class ThriftExample extends AbstractHadoopTestLibrary {

	public static class MyInputProcessor extends InputProcessor<TestEntity, NullWritable> {

		private Tuple outputTuple;

		@Override
    public void setup(Schema schema, org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException,
        InterruptedException, GrouperException {
			outputTuple = TupleFactory.createTuple(context.getConfiguration());
    }

		@Override
    public void process(TestEntity key, NullWritable value,
        com.datasalt.pangolin.grouper.mapreduce.InputProcessor.Collector collector) throws IOException,
        InterruptedException, GrouperException {

			outputTuple.setString("str1", key.getStr1());
			outputTuple.setLong("longnumber", key.getLongNumber());
			outputTuple.setObject("entity", key);
			collector.write(outputTuple);
    }
	}

	public static class MyGroupHandler extends GroupHandler<TestEntity, NullWritable> {

		@Override
		public void onGroupElements(Iterable<ITuple> tuples, org.apache.hadoop.mapreduce.Reducer.Context context)
		    throws IOException, InterruptedException {

			try {
	      context.write(tuples.iterator().next().getObject("entity"), NullWritable.get());
      } catch(InvalidFieldException e) {
      	throw new RuntimeException(e);
      }
		}

		@Override
    public void onOpenGroup(int depth, String field, ITuple firstElement, Context context) throws IOException,
        InterruptedException, GrouperException {
	    
    }

		@Override
    public void onCloseGroup(int depth, String field, ITuple lastElement, Context context) throws IOException,
        InterruptedException, GrouperException {
	    
    }
	}

	public void createInput() throws IOException {
		withInput("I1", testEntity(10, 10, "foo1", "bar1", "bor1"));
		withInput("I1", testEntity(20, 20, "foo1", "bar2", "bor2"));
	}

	public static TestEntity testEntity(long longNumber, int number, String str1, String str2, String str3) {
		TestEntity tEntity = new TestEntity();
		tEntity.setLongNumber(longNumber);
		tEntity.setNumber(number);
		tEntity.setStr1(str1);
		tEntity.setStr2(str2);
		tEntity.setStr3(str3);
		return tEntity;
	}

	@Test
	public void test() throws GrouperException, IOException, InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException {

		createInput();
		
		Schema schema = Schema.parse("str1:string, longnumber:long, number:int, entity:" + TestEntity.class.getName());

		Grouper grouper = new Grouper(getConf());
		grouper.setSchema(schema);

		grouper.addInput(new Path("I1"), SequenceFileInputFormat.class, MyInputProcessor.class);

		SortCriteria sortCriteria = SortCriteria.parse("str1 ASC, longnumber DESC");
		grouper.setSortCriteria(sortCriteria);
		grouper.setOutputFormat(SequenceFileOutputFormat.class);
		grouper.setOutputHandler(MyGroupHandler.class);
		grouper.setFieldsToGroupBy("str1");
		grouper.setOutputKeyClass(TestEntity.class);
		grouper.setOutputValueClass(NullWritable.class);
		Path output = new Path("OUT");

		grouper.setOutputPath(output);
		
		Job job = grouper.createJob();
		HadoopUtils.deleteIfExists(FileSystem.get(getConf()), output);

		assertRun(job);
		
		withOutput(firstReducerOutput("OUT"), testEntity(20, 20, "foo1", "bar2", "bor2"));
	}
}
