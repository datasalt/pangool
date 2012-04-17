//package com.datasalt.pangool.tuplemr.mapred;
//
//import static org.apache.hadoop.io.WritableComparator.compareBytes;
//
//import java.nio.ByteBuffer;
//
//import org.apache.commons.lang.NotImplementedException;
//
//import com.datasalt.pangool.PangoolRuntimeException;
//import com.datasalt.pangool.io.ITuple;
//import com.datasalt.pangool.io.Schema;
//import com.datasalt.pangool.io.Utf8;
//import com.datasalt.pangool.io.Schema.Field;
//import com.datasalt.pangool.io.Schema.Field.FieldSerializer;
//import com.datasalt.pangool.tuplemr.Criteria;
//import com.datasalt.pangool.tuplemr.Criteria.Order;
//import com.datasalt.pangool.tuplemr.Criteria.SortElement;
//
//public class ObjectComparator extends SortComparator{
//
//	@Override
//	public int compare(ITuple w1, ITuple w2) {
//		if(isMultipleSources) {
//			int schemaId1 = tupleMRConf.getSchemaIdByName(w1.getSchema().getName());
//			int schemaId2 = tupleMRConf.getSchemaIdByName(w2.getSchema().getName());
//			int[] indexes1 = serInfo.getCommonSchemaIndexTranslation(schemaId1);
//			int[] indexes2 = serInfo.getCommonSchemaIndexTranslation(schemaId2);
//			Criteria c = tupleMRConf.getCommonCriteria();
//			int comparison = compare(serInfo.getCommonSchema(), c, w1, indexes1, w2, indexes2,serInfo.getCommonSchemaSerializers());
//			if(comparison != 0) {
//				return comparison;
//			} else if(schemaId1 != schemaId2) {
//				int r = schemaId1 - schemaId2;
//				return (tupleMRConf.getSchemasOrder() == Order.ASC) ? r : -r;
//			}
//			int schemaId = schemaId1;
//			c = tupleMRConf.getSpecificOrderBys().get(schemaId);
//			if(c != null) {
//				int[] indexes = serInfo.getSpecificSchemaIndexTranslation(schemaId);
//				return compare(serInfo.getSpecificSchema(schemaId), c, w1, indexes, w2, indexes,serInfo.getSpecificSchemaSerializers().get(schemaId));
//			} else {
//				return 0;
//			}
//		} else {
//			int[] indexes = serInfo.getCommonSchemaIndexTranslation(0);
//			Criteria c = tupleMRConf.getCommonCriteria();
//			return compare(serInfo.getCommonSchema(), c, w1, indexes, w2, indexes,serInfo.getCommonSchemaSerializers());
//		}
//
//	}
//	
//
//	public int compare(Schema schema, Criteria c, ITuple w1, int[] index1, ITuple w2,
//	    int[] index2,FieldSerializer[] serializers) {
//		for(int i = 0; i < c.getElements().size(); i++) {
//			Field field = schema.getField(i);
//			SortElement e = c.getElements().get(i);
//			Object o1 = w1.get(index1[i]);
//			Object o2 = w2.get(index2[i]);
//			FieldSerializer serializer = (serializers == null) ? null : serializers[i];
//			int comparison = compareObjects(o1, o2, e.getCustomComparator(), field.getType(),serializer);
//			if(comparison != 0) {
//				return(e.getOrder() == Order.ASC ? comparison : -comparison);
//			}
//		}
//		return 0;
//	}
//
////	/**
////	 * Compares two objects. Uses the given custom comparator if present. If the
////	 * type is {@link Type#OBJECT} and no raw comparator is present, then a serializer
////	 * comparator is used.
////	 */
////	@SuppressWarnings({ "unchecked" })
////	public int compareObjects(Object elem1, Object elem2, RawComparator comparator,
////	    Type type,FieldSerializer serializer) {
////		// If custom, just use custom.
////		if(comparator != null) {
////			return comparator.compare(elem1, elem2);
////		}
////
////		if(type == Type.OBJECT) {
////			return serializerComparator.compare(elem1,serializer, elem2,serializer);
////		} else {
////			return compareObjects(elem1,elem2);
////		}
////	}
//	
//	public static int compareObjects(Object element1,Object element2){
//		if(element1 == null) {
//			return (element2 == null) ? 0 : -1;
//		} else if(element2 == null) {
//			return 1;
//		} else {
//			if(element1 instanceof String) {
//				element1 = new Utf8((String) element1);
//			}
//			if(element2 instanceof String) {
//				element2 = new Utf8((String) element2);
//			}
//			if(element1 instanceof byte[]){
//				byte[] buffer1 = (byte[])element1;
//				if (element2 instanceof byte[]){
//					byte[] buffer2 = (byte[])element2;
//					return compareBytes(buffer1,0,buffer1.length,buffer2,0,buffer2.length);
//				} else if (element2 instanceof ByteBuffer){
//					ByteBuffer buffer2 = (ByteBuffer)element2;
//					return compareBytes(buffer1,0,buffer1.length,buffer2.array(),buffer2.position(),buffer2.position()+buffer2.limit());
//				} else {
//					throw new PangoolRuntimeException("Can't compare byte[] with " + element2.getClass());
//				}
//			} else if(element1 instanceof ByteBuffer){
//				ByteBuffer buffer1 = (ByteBuffer)element1;
//				if (element2 instanceof byte[]){
//					byte[] buffer2 =(byte[]) element2;
//					return compareBytes(buffer1.array(),buffer1.position(),buffer1.position()+buffer1.limit(),
//							buffer2,0,buffer2.length);
//				} else if (element2 instanceof ByteBuffer){
//					ByteBuffer buffer2 = (ByteBuffer)element2;
//					return compareBytes(buffer1.array(),buffer1.position(),buffer1.position()+buffer1.limit(),
//							buffer2.array(),buffer2.position(),buffer2.position()+buffer2.limit());
//				} else {
//					throw new PangoolRuntimeException("Can't compare byte[] with " + element2.getClass());
//				}
//			}	else if(element1 instanceof Comparable) {
//					return ((Comparable) element1).compareTo(element2);
//			} else if(element2 instanceof Comparable) {
//					return -((Comparable) element2).compareTo(element1);
//			} else {
//				throw new PangoolRuntimeException("Not comparable elements:" + element1.getClass() + " with object " + element2.getClass());
//			} 
//		}
//		
//	}
//	
//	
//	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//		throw new NotImplementedException();
//	}
//}
