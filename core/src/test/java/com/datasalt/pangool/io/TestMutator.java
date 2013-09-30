package com.datasalt.pangool.io;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;

public class TestMutator {

	@Test
	public void testMinus() {
		Schema schema = new Schema("a_schema", Fields.parse("a:int, b:int, c:double, e:string"));
		Schema mutated = Mutator.minusFields("mutated", schema, "b", "e");
		assertEquals(new Schema("mutated", Fields.parse("a:int, c:double")), mutated); 
	}
	
	@Test
	public void testSubSet() {
		Schema schema = new Schema("a_schema", Fields.parse("a:int, b:int, c:double, e:string"));
		Schema mutated = Mutator.subSetOf("mutated", schema, "b", "e");
		assertEquals(new Schema("mutated", Fields.parse("b:int, e:string")), mutated); 
	}

	@Test
	public void testJoint() {
		Schema schema1 = new Schema("a_schema", Fields.parse("a:int, b:int, c:double, e:string"));
		Schema schema2 = new Schema("a_schema", Fields.parse("b:int, f:double, h:string"));
		
		Schema mutated = Mutator.jointSchema("mutated", schema1, schema2);
		
		assertEquals(new Schema("mutated", Fields.parse("a:int, b:int, c:double, e:string, f:double, h:string")), mutated);
	}
	
	@Test
	public void testSuperSet() {
		Schema schema = new Schema("a_schema", Fields.parse("a:int, b:int, c:double, e:string"));
		Schema mutated = Mutator.superSetOf("mutated", schema, Field.create("f", Type.BOOLEAN), Field.create("h", Type.LONG));
		assertEquals(new Schema("mutated", Fields.parse("a:int, b:int, c:double, e:string, f:boolean, h:long")), mutated); 
	}
}
