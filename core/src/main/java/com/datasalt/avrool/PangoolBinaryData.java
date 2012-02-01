package com.datasalt.avrool;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class PangoolBinaryData {

	
	/** If equal, return the number of bytes consumed.  If greater than, return
   * GT, if less than, return LT. */
	

	
  public static int compare(byte[] b1,int s1,byte[] b2,int s2,Schema schema,int[] accumSizes) throws IOException {
    
    switch (schema.getType()) {
    case RECORD: {
      for (Field field : schema.getFields()) {
      	int previousAccumSize1 = accumSizes[0];
      	int previousAccumSize2 = accumSizes[1];
        if (field.order() == Field.Order.IGNORE) {
        	
        	compare(b1,s1,b2,s2,field.schema(),accumSizes);
        } else {
        	int c = compare(b1,s1,b2,s2,field.schema(),accumSizes);
        	if (c != 0){
          	return (field.order() != Field.Order.DESCENDING) ? c : -c;
        	}
        }
        s1 += (accumSizes[0] - previousAccumSize1);
        s2 += (accumSizes[1] - previousAccumSize2);
      }
      return 0;
    }
    case ENUM: case INT: {
      int int1 = WritableComparator.readVInt(b1, s1);
      int int2 = WritableComparator.readVInt(b2, s2);
      accumSizes[0] += WritableUtils.decodeVIntSize(b1[s1]);
      accumSizes[1] += WritableUtils.decodeVIntSize(b2[s2]);
      return int1 == int2 ? 0 : (int1 > int2 ? 1 : -1);
    }
    case LONG: {
      long long1 = WritableComparator.readVLong(b1, s1);
      long long2 = WritableComparator.readVLong(b2, s2);
      accumSizes[0] = WritableUtils.decodeVIntSize(b1[s1]);
      accumSizes[1] = WritableUtils.decodeVIntSize(b2[s2]);
      return long1 == long2 ? 0 : (long1 > long2 ? 1 : -1);
    }
    
    
    // i don't understand this serialization!!
//    case ARRAY: {
//      long i = 0;                                 // position in array
//      long r1 = 0, r2 = 0;                        // remaining in current block
//      long l1 = 0, l2 = 0;                        // total array length
//      while (true) {
//        if (r1 == 0) {                            // refill blocks(s)
//          r1 = d1.readLong();
//          if (r1 < 0) { r1 = -r1; d1.readLong(); }
//          l1 += r1;
//        }
//        if (r2 == 0) {
//          r2 = d2.readLong();
//          if (r2 < 0) { r2 = -r2; d2.readLong(); }
//          l2 += r2;
//        }
//        if (r1 == 0 || r2 == 0)                   // empty block: done
//          return (l1 == l2) ? 0 : ((l1 > l2) ? 1 : -1);
//        long l = Math.min(l1, l2);
//        while (i < l) {                           // compare to end of block
//          int c = compare(d, schema.getElementType());
//          if (c != 0) return c;
//          i++; r1--; r2--;
//        }
//      }
//    }
    case MAP:
      throw new RuntimeException("Can't compare maps!");
    case UNION: {
      int i1 = WritableComparator.readVInt(b1, s1);
      int i2 = WritableComparator.readVInt(b2, s2);
      int vIntSize1 = WritableUtils.decodeVIntSize(b1[s1]);
      int vIntSize2 = WritableUtils.decodeVIntSize(b2[s2]);
      accumSizes[0] += vIntSize1;
      accumSizes[1] += vIntSize2;
      if (i1 == i2) {
      	s1 += vIntSize1;
      	s2 += vIntSize2;
        return compare(b1,s1,b2,s2,schema.getTypes().get(i1),accumSizes);
      } else {
        return i1 - i2;
      }
    }
    case FIXED: {
      int size = schema.getFixedSize();
      accumSizes[0] += size;
      accumSizes[1] += size;
      return WritableComparator.compareBytes(b1, s1, size, b2, s2, size);
    }
    case STRING: case BYTES: {
      int length1 = WritableComparator.readVInt(b1, s1);
      int length2 = WritableComparator.readVInt(b2, s2);
      int vIntSize1 = WritableUtils.decodeVIntSize(b1[s1]);
      int vIntSize2  = WritableUtils.decodeVIntSize(b2[s2]);
      accumSizes[0] += vIntSize1+length1;
      accumSizes[1] += vIntSize2+length2;
      s1+=vIntSize1;
      s2+=vIntSize2;
      return WritableComparator.compareBytes(b1, s1, length1, b2, s2, length2);
    }
    case FLOAT: {
      float f1 = WritableComparator.readFloat(b1, s1);
      float f2 = WritableComparator.readFloat(b2, s2);
      accumSizes[0]+= Float.SIZE/8;
      accumSizes[1]+= Float.SIZE/8;
      return (f1 == f2) ? 0 : ((f1 > f2) ? 1 : -1);
    }
    case DOUBLE: {
      double f1 = WritableComparator.readDouble(b1, s1);
      double f2 = WritableComparator.readDouble(b2, s2);
      accumSizes[0]+= Double.SIZE/8;
      accumSizes[1]+= Double.SIZE/8;
      return (f1 == f2) ? 0 : ((f1 > f2) ? 1 : -1);
    }
    case BOOLEAN:
      boolean boolean1 = (b1[s1] != (byte)0);
      boolean boolean2 = (b1[s1] != (byte)0);
      accumSizes[0]++;
      accumSizes[1]++;
      return (boolean1 == boolean2) ? 0 : (boolean1 ? 1 : -1);
    case NULL:
    	//no accumulate nothing
      return 0;
    default:
      throw new RuntimeException("Unexpected schema to compare!");
    }
  }
  
  
  
	
}
