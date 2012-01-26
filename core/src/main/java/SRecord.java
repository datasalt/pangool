import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import com.datasalt.pangool.io.Serialization;


public class SRecord implements GenericRecord {

	private Serialization ser;
	private GenericRecord contained;
	private DataOutputBuffer outputBuffer = new DataOutputBuffer();
	private DataInputBuffer inputBuffer = new DataInputBuffer();
	
	private Map<String,Class> serializedFields = new HashMap<String,Class>(); //this should be obtained by PangoolConfig
	
	
	public Serialization getSerialization(){
		return ser;
	}
	
	public SRecord(Serialization ser,GenericRecord record) {
	  this.contained = record;
	  this.ser = ser;
  }
	
	
	public void put(String key, Object v,boolean serialize) throws IOException{
		if (serialize){
			ser.ser(v, outputBuffer);
			put(key,outputBuffer.getData()); //TODO super buggy
		} else {
			put(key,v);
		}
	}

	public Object get(String key,boolean deserialize) throws IOException{
		if (deserialize){
			Class clazz = serializedFields.get(key); //TODO super bug
			//inputBuffer.
			return ser.deser(clazz, inputBuffer);
		} else {
			return get(key);
		}
	}

	@Override
  public void put(int i, Object v) {
	  contained.put(i,v);
  }

	@Override
  public Object get(int i) {
		return contained.get(i);
  }

	@Override
  public Schema getSchema() {
		return contained.getSchema();
  }

	@Override
  public void put(String key, Object v) {
		contained.put(key, v);
  }

	@Override
  public Object get(String key) {
		return contained.get(key);
  }
}
