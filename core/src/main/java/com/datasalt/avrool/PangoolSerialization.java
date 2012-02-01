package com.datasalt.avrool;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.avro.Schema;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import java.io.DataInput;
/** The {@link Serialization} used by jobs configured with {@link AvroJob}. */
public class PangoolSerialization<T> implements Serialization<PangoolKey<T>>,Configurable {

	private Schema intermediateSchema;
	private Configuration conf;
	private boolean isDoubleBuffered;
	
	public void setConf(Configuration conf){
		if (conf != null){
			try{
				this.conf = conf;
				CoGrouperConfig grouperConfig = CoGrouperConfig.get(conf);
				isDoubleBuffered = grouperConfig.getRollupFrom() != null;
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
    return PangoolKey.class.isAssignableFrom(c);
  }
  
  /** Returns the specified map output deserializer.  */
  public Deserializer<PangoolKey<T>> getDeserializer(Class<PangoolKey<T>> c) {
//    DatumReader<T> datumReader =
//      getConf().getBoolean(AvroJob.MAP_OUTPUT_IS_REFLECT, false)
//      ? new ReflectDatumReader<T>(intermediateSchema)
//      : new SpecificDatumReader<T>(intermediateSchema);
  	
  	//TODO check this
  	DatumReader<T> datumReader = new SpecificDatumReader<T>(intermediateSchema);
  	
  	
    return new PangoolDeserializer(datumReader,isDoubleBuffered);
  }
  
  private static final DecoderFactory FACTORY = DecoderFactory.get();

  private class PangoolDeserializer
    implements Deserializer<PangoolKey<T>> {

    private DatumReader<T> reader;
    private DataInputDecoder decoder;
    private boolean isDoubleBuffered;
    private InputStream in;
    
    public PangoolDeserializer(DatumReader<T> reader,boolean isDoubleBuffered) {
      this.reader = reader;
      this.isDoubleBuffered = isDoubleBuffered;

    }
    
    public void open(InputStream in) {
    	
      //this.decoder = FACTORY.directBinaryDecoder(in, decoder);
    	this.in = in;
    	this.decoder = new DataInputDecoder((DataInput)in);
    	
    }
    
    public PangoolKey<T> deserialize(PangoolKey<T> wrapper)
      throws IOException {
    	if (wrapper != null && isDoubleBuffered){
    		wrapper.swapInstances();
    	}
      T datum = reader.read(wrapper == null ? null : wrapper.datum(), decoder);
      if (wrapper == null) {
        wrapper = new PangoolKey<T>(datum);
      } else {
        wrapper.datum(datum);
      }
      return wrapper;
    }

    public void close() throws IOException {
    	in.close();
      //decoder.inputStream().close();
    	//((InputStream)decoder.getInputStream()).close();
    }
    
  }
  
  /** Returns the specified output serializer. */
  public Serializer<PangoolKey<T>> getSerializer(Class<PangoolKey<T>> c) {
  	SpecificDatumWriter writer = new SpecificDatumWriter<T>(intermediateSchema);
  	return new PangoolSerializer(writer);
    //return new PangoolSerializer(new ReflectDatumWriter<T>(intermediateSchema));
  }

  private class PangoolSerializer implements Serializer<PangoolKey<T>> {

    private DatumWriter<T> writer;
    private OutputStream out;
    private DataOutputEncoder encoder;
    
    public PangoolSerializer(DatumWriter<T> writer) {
      this.writer = writer;
    }

    public void open(OutputStream out) {
      this.out = out;
      //this.encoder = new EncoderFactory().directBinaryEncoder(out, null);
      //this.encoder = new EncoderFactory().configureBlockSize(512)
      //    .binaryEncoder(out, null);
      this.encoder = new DataOutputEncoder((DataOutput)out);
    }

    public void serialize(PangoolKey<T> wrapper) throws IOException {
      writer.write(wrapper.datum(), encoder);
      // would be a lot faster if the Serializer interface had a flush()
      // method and the Hadoop framework called it when needed rather
      // than for every record.
      
      //encoder.flush();
    }

    public void close() throws IOException {
    	//encoder.flush();
      out.close();
    }
  }

}
