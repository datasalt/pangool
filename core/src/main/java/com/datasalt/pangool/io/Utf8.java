package com.datasalt.pangool.io;

import java.io.Serializable;

import org.apache.hadoop.io.Text;

/**
 * Main String type for Pangool. Natively supported. 
 */
@SuppressWarnings("serial")
public class Utf8 extends Text implements Serializable {

	public Utf8() {
	  super();
  }

	public Utf8(byte[] utf8) {
	  super(utf8);
  }

	public Utf8(String string) {
	  super(string);
  }

	public Utf8(Text utf8) {
	  super(utf8);
  }

	/**
   * If obj is a String or {@link Text}, fills the toFillInCase {@link Utf8}
   * and returns it. Otherwise, returns the object.
	 * @param obj The input object
	 * @param toFillInCase A {@link Utf8} instance to be filled if object is a String
	 * @return toFillInCase instance filled if obj is a String. obj otherwise.
	 */
  public static Object safeForUtf8(Object obj, Utf8 toFillInCase) {
  	if (obj instanceof String) {
  		toFillInCase.set((String) obj);
  		return toFillInCase;
  	} else if (obj instanceof Text) {
  		toFillInCase.set((Text) obj);
  		return toFillInCase;
  	} else {
  		return obj;
  	}
  }
	
}
