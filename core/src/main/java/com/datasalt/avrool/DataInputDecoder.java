package com.datasalt.avrool;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.WritableUtils;

public class DataInputDecoder extends BinaryDecoder{

	private DataInput in;
	
	public DataInputDecoder(DataInput in){
		this.in = in;
	}
	
	public DataInput getInputStream(){
		return in;
	}
	
	
	@Override
  public void readNull() throws IOException {
  }

	@Override
  public boolean readBoolean() throws IOException {
		return (in.readByte() != (byte)0);
  }

	@Override
  public int readInt() throws IOException {
	  return WritableUtils.readVInt(in);
  }

	@Override
  public long readLong() throws IOException {
	  return WritableUtils.readVLong(in);
  }

	@Override
  public float readFloat() throws IOException {
	  return in.readFloat();
  }

	@Override
  public double readDouble() throws IOException {
	  return in.readDouble();
  }

	@Override
  public Utf8 readString(Utf8 old) throws IOException {
		int length = readInt();
		Utf8 result = (old != null ? old : new Utf8());
		result.setByteLength(length);
		in.readFully(result.getBytes(),0, length);
	  return result;
  }

	private final Utf8 scratchUtf8 = new Utf8();
	@Override
  public String readString() throws IOException {
		return readString(scratchUtf8).toString();
  }

	@Override
  public void skipString() throws IOException {
		int length = readInt();
		in.skipBytes(length);
  }

	@Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
		int length = readInt();
    ByteBuffer result;
    if (old != null && length <= old.capacity()) {
      result = old;
      result.clear();
    } else {
      result = ByteBuffer.allocate(length);
    }
    in.readFully(result.array(), result.position(), length);
    result.limit(length);
    return result;
  }

	@Override
  public void skipBytes() throws IOException {
		in.skipBytes(readInt());
	  
  }

	@Override
  public void readFixed(byte[] bytes, int start, int length) throws IOException {
	  in.readFully(bytes,start,length);
	  
  }

	@Override
  public void skipFixed(int length) throws IOException {
	  in.skipBytes(length);
	  
  }

	@Override
  public int readEnum() throws IOException {
	  return readInt();
  }

	@Override
  public long readArrayStart() throws IOException {
		return doReadItemCount();
  }

	@Override
  public long arrayNext() throws IOException {
		return doReadItemCount();
  }

	@Override
  public long skipArray() throws IOException {
		return doSkipItems();
  }

	@Override
  public long readMapStart() throws IOException {
		return doReadItemCount();
  }

	@Override
  public long mapNext() throws IOException {
		return doReadItemCount();
  }

	@Override
  public long skipMap() throws IOException {
	  return doSkipItems();
  }

	@Override
  public int readIndex() throws IOException {
		return readInt();
  }
	
	 protected long doReadItemCount() throws IOException {
	    long result = readLong();
	    if (result < 0) {
	      readLong(); // Consume byte-count if present
	      result = -result;
	    }
	    return result;
	  }
	 
	 private long doSkipItems() throws IOException {
	    long result = readInt();
	    while (result < 0) {
	      long bytecount = readLong();
	      doSkipBytes(bytecount);
	      result = readInt();
	    }
	    return result;
	  }
	 
	 protected void doSkipBytes(long length) throws IOException {
		 in.skipBytes((int)length);
//	    int remaining = limit - pos;
//	    if (length <= remaining) {
//	      pos += length;
//	    } else {
//	      limit = pos = 0;
//	      length -= remaining;
//	      source.skipSourceBytes(length);
//	    }
	  }

}
