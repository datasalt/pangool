//package com.datasalt.avrool.api;
//
//import java.io.IOException;
//
//import org.apache.avro.generic.GenericData.Record;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.hadoop.io.NullWritable;
//
//import com.datasalt.avrool.CoGrouperException;
//
//public class IdentityGroupHandler extends GroupHandler<AvroKey, AvroValue> {
//
//	@Override
//	public void onGroupElements(GenericRecord group, Iterable<GenericRecord> tuples, CoGrouperContext<AvroKey, AvroValue> context,
//	    Collector<Record, NullWritable> collector) throws IOException, InterruptedException, CoGrouperException {
//
//		for(Record tuple : tuples) {
//			collector.write(tuple, NullWritable.get());
//		}
//	}
//}
