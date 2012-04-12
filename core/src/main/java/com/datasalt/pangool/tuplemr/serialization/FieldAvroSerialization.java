package com.datasalt.pangool.tuplemr.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import com.datasalt.pangool.io.Schema.Field.FieldDeserializer;
import com.datasalt.pangool.io.Schema.Field.FieldSerializer;

public class FieldAvroSerialization {
	
	public static class AvroFieldSerializer implements FieldSerializer{

		private DatumWriter writer;
    private OutputStream out;
    private BinaryEncoder encoder;

		@Override
		public void open(OutputStream out) throws IOException {
			this.out = out;
      this.encoder = new EncoderFactory().configureBlockSize(512)
          .binaryEncoder(out, null);
			
		}

		@Override
		public void serialize(Object object) throws IOException {
			writer.write(object, encoder);
      // would be a lot faster if the Serializer interface had a flush()
      // method and the Hadoop framework called it when needed rather
      // than for every record.
      encoder.flush();
			
		}
		
		@Override
		public void close() throws IOException {
			out.close();
		}

		@Override
		public void setProps(Map<String, String> properties) {
	    Schema schema = Schema.parse(properties.get("avro.schema"));
	    String r = properties.get("avro.reflection");
	    boolean isReflect = (r != null) && Boolean.parseBoolean(r);
	    writer = (isReflect) ?
	       new ReflectDatumWriter(schema)
	      : new SpecificDatumWriter(schema);
		}
		
	}
	
	public static class AvroFieldDeserializer implements FieldDeserializer{

		private static final DecoderFactory FACTORY = DecoderFactory.get();
		private DatumReader reader;
    private BinaryDecoder decoder;
    
    public AvroFieldDeserializer(){
    }
		
		@Override
		public void close() throws IOException {
			decoder.inputStream().close();
		}

		@Override
		public Object deserialize(Object object) throws IOException {
			return reader.read(object, decoder);
		}

		@Override
		public void open(InputStream in) throws IOException {
			this.decoder = FACTORY.directBinaryDecoder(in, decoder);
		}

		@Override
		public void setProps(Map<String, String> properties) {
			Schema schema = Schema.parse(properties.get("avro.schema"));
			String r = properties.get("avro.reflection");
	    boolean isReflect = (r != null) && Boolean.parseBoolean(r);
			reader = (isReflect) ? new ReflectDatumReader(schema)
          : new SpecificDatumReader(schema);
		}
		
	}

}
