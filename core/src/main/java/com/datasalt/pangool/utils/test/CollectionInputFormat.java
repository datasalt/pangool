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
package com.datasalt.pangool.utils.test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * A base input format for use a {@link Collection} as input for
 * a MapReduce Job. Useful for unit testing. Example of
 * usage:
 * <code>
 * 	public static class Input extends CollectionInputFormat {
 *		
 *		public Input() {}
 *		
 *    public Collection mapToServe() {
 *			ArrayList<String> data = new ArrayList();
 *			data.put(new Duple(new LongWritable(1),new Text("Hola colega")));
 *			data.put(new Duple(new LongWritable(2),new Text("De la vega")));
 *			return data;
 *    }		
 *	}
 *
 * ...
 *	job.setInputFormatClass(Input.class);
 * ...
 * </code>
 *
 * @param <K>
 * @param <V>
 */
public abstract class CollectionInputFormat<K,V> extends InputFormat<K,V> {

	public CollectionInputFormat(){}
	
	/**
	 * Override and return the list of Key/Value pairs to be served by
	 * this input format.
	 */
	public abstract Collection<Entry<K, V>> dataToServe(); 
	
	public static class CustomInputSplit extends InputSplit implements Writable {
		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return new String[]{""};
		}
		
		@Override
		public long getLength() throws IOException, InterruptedException {
			return 1;
		}

		@Override
    public void write(DataOutput out) throws IOException {
    }

		@Override
    public void readFields(DataInput in) throws IOException {
    }		
	}
	
	public static class Duple<K,V> implements Entry<K,V> {
		K k;
		V v;
		
		public Duple(K k, V v) {
			this.k = k;
			this.v = v;
		}
		
		@Override
    public K getKey() {
			return k;
    }

		@Override
    public V getValue() {
			return v;
    }

		@Override
    public V setValue(V value) {
			return (v = value);
    }		
	}
	
	@Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		ArrayList<InputSplit> ret = new ArrayList<InputSplit>();
		ret.add(new CustomInputSplit());
		return ret;
  }

	@Override
  public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) 
  throws IOException, InterruptedException {
		return new RecordReader<K, V>() {
			Iterator<Entry<K, V>> it;
			Entry<K,V> currentEntry;
			int count = 0;
			int total;
			
			@Override
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
				Collection<Entry<K, V>> entries = dataToServe();
				total = entries.size();
				it = entries.iterator();
      }

			@Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
				if (it.hasNext()) {
					currentEntry = it.next();
					return true;
				} else {
					return false;
				}
      }

			@Override
      public K getCurrentKey() throws IOException, InterruptedException {
	      return currentEntry.getKey();
      }

			@Override
      public V getCurrentValue() throws IOException, InterruptedException {
				return currentEntry.getValue();
      }

			@Override
      public float getProgress() throws IOException, InterruptedException {
	      return count/(float)total;
      }

			@Override
      public void close() throws IOException {
      }			
		};
  }
}
