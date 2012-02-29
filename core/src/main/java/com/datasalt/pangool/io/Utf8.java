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
}
