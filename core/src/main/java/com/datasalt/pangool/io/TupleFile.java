/**
 * Copyright [2012] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datasalt.pangool.io;

import com.datasalt.pangool.serialization.HadoopSerialization;
import com.datasalt.pangool.tuplemr.serialization.SimpleTupleDeserializer;
import com.datasalt.pangool.tuplemr.serialization.SimpleTupleSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Progressable;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Utilities class for reading and writing binary files with {@link ITuple}.
 * {@link SequenceFile}s and their ability to manage {@link org.apache.hadoop.io.SequenceFile.Metadata}
 * is used to read and write the files. As it is implemented using {@link SequenceFile}s,
 * it supports compression.
 *
 * <br>
 * See classes {@link Writer} and {@link Reader} to see more info about how to
 * write and read TupleFiles
 */
public class TupleFile {

  /**
   * Class for writing files containing {@link ITuple}. Typical usage would be:
   * <br/>
   * <code>
   *   TupleFile.Writer writer = new TupleFile.Writer(fs, conf, file, schema);
   *   Tuple tuple = new Tuple(schema);
   *   for (...) {
   *     fillTuple(tuple);
   *     writer.append(tuple);
   *   }
   *   close();
   * </code>
   */
  public static class Writer implements java.io.Closeable {

    @SuppressWarnings("rawtypes")
    private static Class UNUSED = Text.class;

    private SequenceFile.Writer innerWriter;
    private SimpleTupleSerializer ser;
    private DataOutputBuffer outputBuffer;

    /**
     * Create the named file for storing @{link ITuple}s with the given schema.
     */
    public Writer(FileSystem fs, Configuration conf, Path name, Schema schema)
        throws IOException {
      this(fs, conf, name, schema, null,
          new SequenceFile.Metadata());
    }

    /**
     * Create the named file with write-progress reporter for storing @{link ITuple}s with the given schema.
     */
    public Writer(FileSystem fs, Configuration conf, Path name, Schema schema,
                  Progressable progress, SequenceFile.Metadata metadata)
        throws IOException {
      this(fs, conf, name, schema,
          fs.getConf().getInt("io.file.buffer.size", 4096),
          fs.getDefaultReplication(), fs.getDefaultBlockSize(),
          progress, metadata);
    }

    /**
     * Create the named file with write-progress reporter for storing @{link ITuple}s with the given schema.
     */
    public Writer(FileSystem fs, Configuration conf, Path name, Schema schema,
                  int bufferSize, short replication, long blockSize,
                  Progressable progress, SequenceFile.Metadata metadata)
        throws IOException {
      fillMetadata(metadata, schema);
      innerWriter = new SequenceFile.Writer(fs, conf, name, UNUSED, UNUSED, bufferSize, replication,
          blockSize, progress, metadata);
      init(conf, schema);
    }

    /**
     * Creates a TupleFile Writer.
     * @param fs The configured filesystem.
     * @param conf The configuration.
     * @param name The name of the file.
     * @param schema The schema of the tuples to be written
     * @param bufferSize buffer size for the underlaying outputstream.
     * @param replication replication factor for the file.
     * @param blockSize block size for the file.
     * @param compressionType The compression type.
     * @param codec The compression codec.
     * @param progress The Progressable object to track progress.
     * @param metadata The metadata of the file.
     * @throws IOException
     */
    public Writer(FileSystem fs, Configuration conf, Path name,
                 Schema schema, int bufferSize,
                 short replication, long blockSize,
                 SequenceFile.CompressionType compressionType, CompressionCodec codec,
                 Progressable progress, SequenceFile.Metadata metadata) throws IOException {
      fillMetadata(metadata, schema);
      innerWriter = SequenceFile.createWriter(fs, conf, name, UNUSED, UNUSED, bufferSize, replication,
          blockSize, compressionType, codec, progress, metadata);
      init(conf, schema);
    }

    /**
     * Creates a TupleFile Writer.
     * @param conf The configuration.
     * @param out The stream on top which the writer is to be constructed.
     * @param schema The schema of the tuples to be written
     * @param compressionType The compression type.
     * @param codec The compression codec.
     * @param metadata The metadata of the file.
     */
    public Writer(Configuration conf, FSDataOutputStream out,
                 Schema schema, SequenceFile.CompressionType compressionType,
                 CompressionCodec codec, SequenceFile.Metadata metadata) throws IOException {
      fillMetadata(metadata, schema);
      innerWriter = SequenceFile.createWriter(conf, out, UNUSED, UNUSED, compressionType, codec,
          metadata);
      init(conf, schema);
    }

    /**
     * Creates a TupleFile Writer.
     * @param fs The configured filesystem.
     * @param conf The configuration.
     * @param name The name of the file.
     * @param schema The schema of the tuples to be written
     * @param compressionType The compression type.
     * @param codec The compression codec.
     * @param progress The Progressable object to track progress.
     */
    public Writer(FileSystem fs, Configuration conf, Path name,
                  Schema schema,
                  SequenceFile.CompressionType compressionType, CompressionCodec codec,
                  Progressable progress) throws IOException {
      this(fs, conf, name, schema, compressionType, codec, progress, new SequenceFile.Metadata());
    }

    /**
     * Creates a TupleFile Writer.
     * @param fs The configured filesystem.
     * @param conf The configuration.
     * @param name The name of the file.
     * @param schema The schema of the tuples to be written
     * @param compressionType The compression type.
     * @param codec The compression codec.
     * @param progress The Progressable object to track progress.
     * @param metadata The metadata of the file.
     */
    public Writer(FileSystem fs, Configuration conf, Path name,
                 Schema schema,
                 SequenceFile.CompressionType compressionType, CompressionCodec codec,
                 Progressable progress, SequenceFile.Metadata metadata) throws IOException {
      fillMetadata(metadata, schema);
      innerWriter = SequenceFile.createWriter(fs, conf, name, UNUSED, UNUSED, compressionType, codec, progress,metadata);
      init(conf, schema);
    }

    private void init(Configuration conf, Schema schema) throws IOException {
      HadoopSerialization hadoopSer = new HadoopSerialization(conf);
      this.ser = new SimpleTupleSerializer(schema, hadoopSer, conf);
      this.outputBuffer = new DataOutputBuffer();
      ser.open(outputBuffer);
    }

    /**
     * Returns the compression codec of data in this file.
     */
    public CompressionCodec getCompressionCodec() {
      return innerWriter.getCompressionCodec();
    }

    /**
     * create a sync point
     */
    public void sync() throws IOException {
      innerWriter.sync();
    }

    /**
     * TODO: Uncomment when Hadoop version increased.
     */
    /* public void syncFs() throws IOException {
      innerWriter.syncFs();
    } */
    private static SequenceFile.Metadata fillMetadata(SequenceFile.Metadata metadata, Schema schema) {
      metadata.set(new Text("schema"), new Text(schema.toString()));
      return metadata;
    }

    /**
     * Close the file.
     */
    public synchronized void close() throws IOException {
      outputBuffer.close();
      ser.close();
      innerWriter.close();
    }

    /**
     * Append a {@link ITuple}
     */
    public synchronized void append(ITuple tuple)
        throws IOException {
      outputBuffer.reset();
      ser.serialize(tuple);
      innerWriter.appendRaw(outputBuffer.getData(), 0, outputBuffer.getLength(), new SequenceFile.ValueBytes() {
        @Override
        public void writeUncompressedBytes(DataOutputStream outStream) throws IOException {
        }

        @Override
        public void writeCompressedBytes(DataOutputStream outStream) throws IllegalArgumentException, IOException {
        }

        @Override
        public int getSize() {
          return 0;
        }
      });
    }

    /**
     * Returns the current length of the output file.
     * <p/>
     * <p>This always returns a synchronized position.  In other words,
     * immediately after calling {@link TupleFile.Reader#seek(long)} with a position
     * returned by this method, {@link TupleFile.Reader#next(ITuple)} may be called.  However
     * the key may be earlier in the file than key last written when this
     * method was called (e.g., with block-compression, it may be the first key
     * in the block that was being written when this method was called).
     */
    public synchronized long getLength() throws IOException {
      return innerWriter.getLength();
    }
  } // class Writer

  /**
   * Reads SequenceFiles containing @{link ITuple} written using {@link TupleFile.Writer}.
   * Typical usage would be:
   * <br>
   * <code>
   *  TupleFile.Reader reader = new TupleFile.Reader(fs, file, conf);
   *  Tuple tuple = new Tuple(reader.getSchema());
   *  while (reader.next(tuple) {
   *    ....
   *  }
   *  reader.close();
   * </code>
   */
  public static class Reader implements java.io.Closeable {

    private SequenceFile.Reader innerReader;
    private Schema schema;
    private Path file;

    private SimpleTupleDeserializer deser;
    private DataInputBuffer inBuffer = new DataInputBuffer();
    private DataOutputBuffer outBuffer = new DataOutputBuffer();
    ITuple tuple;

    /**
     * Open the named file. A specific Schema will be used in a backwards-compatible way.
     * The Schema in the file will be read according to it, so that unused fields will be
     * skipped and new fields will be initialized as null.
     */
    public Reader(FileSystem fs, Schema destSchema, Configuration conf, Path file) throws IOException {
      this.file = file;
      innerReader = new SequenceFile.Reader(fs, file, conf);
      loadSchema();

      HadoopSerialization ser = new HadoopSerialization(conf);
      if(destSchema != null) {
        this.deser = new SimpleTupleDeserializer(schema, destSchema, ser, conf);
        this.tuple = new Tuple(destSchema);
      } else {
        this.deser = new SimpleTupleDeserializer(schema, ser, conf);      	
        this.tuple = new Tuple(schema);
      }

      deser.open(inBuffer);    	
    }
    
    /**
     * Open the named file.
     */
    public Reader(FileSystem fs, Configuration conf, Path file)
        throws IOException {
    	this(fs, null, conf, file);
    }

    private void loadSchema() throws IOException {
      SequenceFile.Metadata meta = innerReader.getMetadata();
      Text schemaText = meta.get(new Text("schema"));

      if (schemaText == null) {
        throw new IOException("Invalid Sequence File with Tuples [" + file + "] : it does not contain the tuple's schema in the metadata");
      }

      try {
        schema = Schema.parse(schemaText.toString());
      } catch (Schema.SchemaParseException e) {
        throw new IOException("Invalid Schema found in file: " + file + ". Schema: " + schemaText.toString());
      }
    }

    /**
     * Close the file.
     */
    public synchronized void close() throws IOException {
      deser.close();
      inBuffer.close();
      innerReader.close();
    }

    /**
     * Returns true if values are compressed.
     */
    public boolean isCompressed() {
      return innerReader.isCompressed();
    }

    /**
     * Returns true if records are block-compressed.
     */
    public boolean isBlockCompressed() {
      return innerReader.isBlockCompressed();
    }

    /**
     * Returns the compression codec of data in this file.
     */
    public CompressionCodec getCompressionCodec() {
      return innerReader.getCompressionCodec();
    }

    /**
     * Returns the metadata object of the file
     */
    public SequenceFile.Metadata getMetadata() {
      return innerReader.getMetadata();
    }

    /**
     * Return the tuple's {@link Schema} in the file.
     */
    public Schema getSchema() {
      return schema;
    }

    /**
     * Read the next {@link ITuple} in the file into <code>tuple</code>.
     * True if another entry exists, and false at end of file.
     */
    public synchronized boolean next(ITuple tuple) throws IOException {
      outBuffer.reset();
      int i = innerReader.nextRawKey(outBuffer);
      if (i < 0) {
        return false;
      }
      inBuffer.reset(outBuffer.getData(), outBuffer.getLength());
      deser.deserialize(tuple);
      return true;
    }

    /**
     * Set the current byte position in the input file.
     * <p/>
     * <p>The position passed must be a position returned by {@link
     * TupleFile.Writer#getLength()} when writing this file.  To seek to an arbitrary
     * position, use {@link TupleFile.Reader#sync(long)}.
     */
    public synchronized void seek(long position) throws IOException {
      innerReader.seek(position);
    }

    /**
     * Seek to the next sync mark past a given position.
     */
    public synchronized void sync(long position) throws IOException {
      innerReader.sync(position);
    }

    /**
     * Returns true iff the previous call to next passed a sync mark.
     */
    public boolean syncSeen() {
      return innerReader.syncSeen();
    }

    /**
     * Return the current byte position in the input file.
     */
    public synchronized long getPosition() throws IOException {
      return innerReader.getPosition();
    }

    /**
     * Returns the name of the file.
     */
    public String toString() {
      return file.toString();
    }
  }
}
