package com.datasalt.avrool;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

/** The {@link Serialization} used by jobs configured with {@link AvroJob}. */
public class PangoolSerialization<T> implements Serialization<PangoolWrapper<T>>,Configurable {

	private Schema intermediateSchema;
	private Configuration conf;
	
	public void setConf(Configuration conf){
		if (conf != null){
			try{
				this.conf = conf;
				CoGrouperConfig grouperConfig = CoGrouperConfig.get(conf);
				SerializationInfo serInfo = SerializationInfo.get(grouperConfig);
				intermediateSchema = serInfo.getIntermediateSchema();
			} catch(CoGrouperException e){
				throw new RuntimeException(e);
			}
		}
	}
	
	public Configuration getConf(){
		return conf;
	}
	
  public boolean accept(Class<?> c) {
    return PangoolWrapper.class.isAssignableFrom(c);
  }
  
  /** Returns the specified map output deserializer.  */
  public Deserializer<PangoolWrapper<T>> getDeserializer(Class<PangoolWrapper<T>> c) {
//    DatumReader<T> datumReader =
//      getConf().getBoolean(AvroJob.MAP_OUTPUT_IS_REFLECT, false)
//      ? new ReflectDatumReader<T>(intermediateSchema)
//      : new SpecificDatumReader<T>(intermediateSchema);
  	
  	DatumReader<T> datumReader = new SpecificDatumReader<T>(intermediateSchema);
  	
    return new PangoolDeserializer(datumReader);
  }
  
  private static final DecoderFactory FACTORY = DecoderFactory.get();

  private class PangoolDeserializer
    implements Deserializer<PangoolWrapper<T>> {

    private DatumReader<T> reader;
    private BinaryDecoder decoder;
    
    public PangoolDeserializer(DatumReader<T> reader) {
      this.reader = reader;

    }
    
    public void open(InputStream in) {
      this.decoder = FACTORY.directBinaryDecoder(in, decoder);
    }
    
    public PangoolWrapper<T> deserialize(PangoolWrapper<T> wrapper)
      throws IOException {
      T datum = reader.read(wrapper == null ? null : wrapper.datum(), decoder);
      if (wrapper == null) {
        wrapper = new PangoolWrapper<T>(datum);
      } else {
        wrapper.datum(datum);
      }
      return wrapper;
    }

    public void close() throws IOException {
      decoder.inputStream().close();
    }
    
  }
  
  /** Returns the specified output serializer. */
  public Serializer<PangoolWrapper<T>> getSerializer(Class<PangoolWrapper<T>> c) {
    return new PangoolSerializer(new ReflectDatumWriter<T>(intermediateSchema));
  }

  private class PangoolSerializer implements Serializer<PangoolWrapper<T>> {

    private DatumWriter<T> writer;
    private OutputStream out;
    private BinaryEncoder encoder;
    
    public PangoolSerializer(DatumWriter<T> writer) {
      this.writer = writer;
    }

    public void open(OutputStream out) {
      this.out = out;
      this.encoder = new EncoderFactory().configureBlockSize(512)
          .binaryEncoder(out, null);
    }

    public void serialize(PangoolWrapper<T> wrapper) throws IOException {
      writer.write(wrapper.datum(), encoder);
      // would be a lot faster if the Serializer interface had a flush()
      // method and the Hadoop framework called it when needed rather
      // than for every record.
      encoder.flush();
    }

    public void close() throws IOException {
      out.close();
    }
  }

}
