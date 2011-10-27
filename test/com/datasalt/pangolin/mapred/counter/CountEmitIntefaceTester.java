package com.datasalt.pangolin.mapred.counter;

import static com.datasalt.pangolin.io.IdDatumPairBase.newOne;
import static com.datasalt.pangolin.commons.ObjectUtils.toJSON;

import java.io.IOException;
import java.util.ArrayList;



import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;

import com.datasalt.pangolin.io.IdDatumPairBase;
import com.datasalt.pangolin.io.Serialization;

/**
 * Class for testing the {@link Accountant}
 * 
 * @author ivan
 */
public class CountEmitIntefaceTester implements CountEmitInterface {

	int count = 0;
  ArrayList<IdDatumPairBase> expected = new ArrayList<IdDatumPairBase>();
	Serialization ser;
	
	public CountEmitIntefaceTester(Configuration conf) throws IOException {
		ser = new Serialization(conf);
	}
	
	CountEmitIntefaceTester neW(Configuration conf) throws IOException {
		return new CountEmitIntefaceTester(conf);
	}

	
  public CountEmitIntefaceTester add(IdDatumPairBase pair) {
		expected.add(pair);
		return this;
	}
	
	public void add(int typeIdentifier, Object group, Object item) throws TException, IOException {
		expected.add(newOne(typeIdentifier, ser.ser(group), ser.ser(item)));
	}

	
  private void assertCorrect(
      IdDatumPairBase newOne) {
		if (count >= expected.size()) {
			throw new RuntimeException("Too many emisions: " + count + ". Expected " + expected.size());
		}
		IdDatumPairBase expectedOne = expected.get(count++);
		
		if (!newOne.equals(expectedOne)) {
			throw new RuntimeException("Expected " + toJSON(expectedOne) + " but obtained " + toJSON(newOne));
		}
  }
	
  public void close() {
		if (count != expected.size()) {
			throw new RuntimeException("Too few emisions: " + count + ". Expected " + expected.size());
		}		
	}
	
	@Override
  public void emit(int typeIdentifier, Object group, Object item)
      throws IOException, InterruptedException {
	    assertCorrect(newOne(typeIdentifier, ser.ser(group), ser.ser(item)));
  }

}
