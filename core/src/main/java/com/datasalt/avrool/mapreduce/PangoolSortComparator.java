package com.datasalt.avrool.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.BinaryData;
import org.apache.avro.mapred.AvroKeyComparator;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;

import com.datasalt.avrool.CoGrouperConfig;
import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.PangoolBinaryData;
import com.datasalt.avrool.PangoolKey;
import com.datasalt.avrool.SerializationInfo;

public class PangoolSortComparator implements RawComparator<PangoolKey>,Configurable {

		private Schema schema;
		private int[] accumSizes=new int[]{0,0};
		private Configuration conf;
		
		@Override
		public void setConf(Configuration conf) {
			if(conf != null) {
				this.conf = conf;
				CoGrouperConfig grouperConfig;
        try {
	        grouperConfig = CoGrouperConfig.get(conf);
       
				SerializationInfo serInfo = SerializationInfo.get(grouperConfig);
				schema = serInfo.getSortSchema();
				if (schema == null){
					//TODO deprecated
					schema = serInfo.getIntermediateSchema();
				}
        } catch(CoGrouperException e) {
	       throw new RuntimeException(e);
        }
			}
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)  {
			
			//return BinaryData.compare(b1, s1, l1, b2, s2, l2, schema); 
//		
			//System.out.println("(" + s1 + "," + l1 + ") ; (" + s2 + "," + l2 + ") => " + comparison);
			//return comparison;
			try{
				//accumSizes[0] = (accumSizes[1] = 0); //todo not needed
			return PangoolBinaryData.compare(b1, s1, b2, s2, schema, accumSizes);
		} catch(IOException e){
			throw new RuntimeException(e);
		}	
		}

//		public int compare(PangoolKey x, PangoolKey y) {
//			throw new RuntimeException("Not implemented");
//			//return ReflectData.get().compare(x.datum(), y.datum(), schema);
//		}

		@Override
    	public Configuration getConf() {
			return this.conf;
    }

		@Override
    public int compare(PangoolKey arg0, PangoolKey arg1) {
	    throw new RuntimeException("Not implemented");
    }
	}
