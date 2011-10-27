package com.datasalt.pangolin.commons;

import static org.junit.Assert.*;
import static com.datasalt.pangolin.commons.WritableUtils.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

public class TestWritableUtils {

	private byte [] writeShort(short s) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    dos.writeShort(s);
    return bos.toByteArray();
	}
	
	@Test
	public void testReadShort() throws IOException {
	
		short s = (short) -1;
    byte [] b1 = writeShort(s);
    assertEquals(s, readShort(b1, 0));
    
    s = 1;
    b1 = writeShort(s);
    assertEquals(s, readShort(b1, 0));

    s = 1000;
    b1 = writeShort(s);
    assertEquals(s, readShort(b1, 0));

    s = -1000;
    b1 = writeShort(s);
    assertEquals(s, readShort(b1, 0));
    
    s = Short.MAX_VALUE;
    b1 = writeShort(s);
    assertEquals(s, readShort(b1, 0));


    s = Short.MIN_VALUE;
    b1 = writeShort(s);
    assertEquals(s, readShort(b1, 0));		
	}
	
}
