//package com.datasalt.avrool;
//
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.OutputStream;
//
//import org.apache.hadoop.io.serializer.Serialization;
//import org.apache.hadoop.io.serializer.Deserializer;
//import org.apache.hadoop.io.serializer.Serializer;
//import org.apache.hadoop.conf.Configured;
//
//import org.apache.avro.Schema;
//import org.apache.avro.io.BinaryEncoder;
//import org.apache.avro.io.BinaryDecoder;
//import org.apache.avro.io.DecoderFactory;
//import org.apache.avro.io.DatumReader;
//import org.apache.avro.io.DatumWriter;
//import org.apache.avro.io.EncoderFactory;
//import org.apache.avro.specific.SpecificDatumReader;
//import org.apache.avro.reflect.ReflectDatumReader;
//import org.apache.avro.reflect.ReflectDatumWriter;
//
///** The {@link Serialization} used by jobs configured with {@link AvroJob}. */
//public class PangoolSerialization<T> extends Configured 
//  implements Serialization<PangoolWrapper<T>> {
//
//  public boolean accept(Class<?> c) {
//    return PangoolWrapper.class.isAssignableFrom(c);
//  }
//  
//  /** Returns the specified map output deserializer.  Defaults to the final
//   * output deserializer if no map output schema was specified. */
//  public Deserializer<PangoolWrapper<T>> getDeserializer(Class<PangoolWrapper<T>> c) {
//    boolean isKey = AvroKey.class.isAssignableFrom(c);
//    Schema schema = isKey
//      ? Pair.getKeySchema(AvroJob.getMapOutputSchema(getConf()))
//      : Pair.getValueSchema(AvroJob.getMapOutputSchema(getConf()));
//    DatumReader<T> datumReader =
//      getConf().getBoolean(AvroJob.MAP_OUTPUT_IS_REFLECT, false)
//      ? new ReflectDatumReader<T>(schema)
//      : new SpecificDatumReader<T>(schema);
//    return new AvroWrapperDeserializer(datumReader, isKey);
//  }
//  
//  private static final DecoderFactory FACTORY = DecoderFactory.get();
//
//  private class AvroWrapperDeserializer
//    implements Deserializer<AvroWrapper<T>> {
//
//    private DatumReader<T> reader;
//    private BinaryDecoder decoder;
//    private boolean isKey;
//    
//    public AvroWrapperDeserializer(DatumReader<T> reader, boolean isKey) {
//      this.reader = reader;
//      this.isKey = isKey;
//    }
//    
//    public void open(InputStream in) {
//      this.decoder = FACTORY.directBinaryDecoder(in, decoder);
//    }
//    
//    public AvroWrapper<T> deserialize(AvroWrapper<T> wrapper)
//      throws IOException {
//      T datum = reader.read(wrapper == null ? null : wrapper.datum(), decoder);
//      if (wrapper == null) {
//        wrapper = isKey? new AvroKey<T>(datum) : new AvroValue<T>(datum);
//      } else {
//        wrapper.datum(datum);
//      }
//      return wrapper;
//    }
//
//    public void close() throws IOException {
//      decoder.inputStream().close();
//    }
//    
//  }
//  
//  /** Returns the specified output serializer. */
//  public Serializer<AvroWrapper<T>> getSerializer(Class<AvroWrapper<T>> c) {
//    // AvroWrapper used for final output, AvroKey or AvroValue for map output
//    boolean isFinalOutput = c.equals(AvroWrapper.class);
//    Schema schema = isFinalOutput
//      ? AvroJob.getOutputSchema(getConf())
//      : (AvroKey.class.isAssignableFrom(c)
//         ? Pair.getKeySchema(AvroJob.getMapOutputSchema(getConf()))
//         : Pair.getValueSchema(AvroJob.getMapOutputSchema(getConf())));
//    return new AvroWrapperSerializer(new ReflectDatumWriter<T>(schema));
//  }
//
//  private class AvroWrapperSerializer implements Serializer<AvroWrapper<T>> {
//
//    private DatumWriter<T> writer;
//    private OutputStream out;
//    private BinaryEncoder encoder;
//    
//    public AvroWrapperSerializer(DatumWriter<T> writer) {
//      this.writer = writer;
//    }
//
//    public void open(OutputStream out) {
//      this.out = out;
//      this.encoder = new EncoderFactory().configureBlockSize(512)
//          .binaryEncoder(out, null);
//    }
//
//    public void serialize(AvroWrapper<T> wrapper) throws IOException {
//      writer.write(wrapper.datum(), encoder);
//      // would be a lot faster if the Serializer interface had a flush()
//      // method and the Hadoop framework called it when needed rather
//      // than for every record.
//      encoder.flush();
//    }
//
//    public void close() throws IOException {
//      out.close();
//    }
//
//  }
//
//}
