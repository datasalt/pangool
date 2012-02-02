package com.datasalt.avrool;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.WritableUtils;

public class DataOutputEncoder extends BinaryEncoder{

	private DataOutput out;
	
	public DataOutputEncoder(DataOutput out){
		this.out = out;
	}
	
	  
	
	@Override
  public void flush() throws IOException {
	  
	  
  }

	@Override
  public void writeNull() throws IOException {
	  //nothing
	  
  }

	@Override
  public void writeBoolean(boolean b) throws IOException {
	  out.writeBoolean(b);
	  
  }

	@Override
  public void writeInt(int n) throws IOException {
	  WritableUtils.writeVInt(out, n);
	  
  }

	@Override
  public void writeLong(long n) throws IOException {
	  WritableUtils.writeVLong(out, n);
	  
  }

	@Override
  public void writeFloat(float f) throws IOException {
	  out.writeFloat(f);
	  
  }

	@Override
  public void writeDouble(double d) throws IOException {
	  out.writeDouble(d);
	  
  }

	@Override
  public void writeString(Utf8 utf8) throws IOException {
		//writeInt(utf8.getByteLength());
		writeBytes(utf8.getBytes(), 0, utf8.getByteLength());
	  
  }

	@Override
  public void writeBytes(ByteBuffer bytes) throws IOException {
		int pos = bytes.position();
    int start = bytes.arrayOffset() + pos;
    int len = bytes.limit() - pos;
    writeBytes(bytes.array(), start, len);
	  
  }

	@Override
  public void writeBytes(byte[] bytes, int start, int len) throws IOException {
		writeInt(len);
	  out.write(bytes,start,len);
	  
  }

	@Override
  public void writeFixed(byte[] bytes, int start, int len) throws IOException {
	  out.write(bytes,start,len);
	  
  }

	@Override
  public void writeEnum(int e) throws IOException {
	  WritableUtils.writeVInt(out, e);
	  
  }

	@Override
  public void writeArrayStart() throws IOException {
	  // TODO Auto-generated method stub
	  
  }

	@Override
  public void setItemCount(long itemCount) throws IOException {
		if (itemCount > 0) {
      this.writeLong(itemCount);
    }
  }

	@Override
  public void startItem() throws IOException {
	  // TODO Auto-generated method stub
	  
  }

	@Override
  public void writeArrayEnd() throws IOException {
		writeZero();
	  
  }

	@Override
  public void writeMapStart() throws IOException {
	  // TODO Auto-generated method stub
	  
  }

	@Override
  public void writeMapEnd() throws IOException {
		writeZero();
	  
  }

	@Override
  public void writeIndex(int unionIndex) throws IOException {
		writeInt(unionIndex);
	  
  }
	
	protected void writeZero() throws IOException {
    out.writeByte(0);
  }


	@Override
  public int bytesBuffered() {
	  return 0;
  }

}
