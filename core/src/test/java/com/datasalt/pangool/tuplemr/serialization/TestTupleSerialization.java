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
package com.datasalt.pangool.tuplemr.serialization;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.datasalt.pangool.BaseTest;
import com.datasalt.pangool.io.DatumWrapper;
import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.serialization.HadoopSerialization;
import com.datasalt.pangool.thrift.test.A;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRConfig;
import com.datasalt.pangool.tuplemr.TupleMRConfigBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;


public class TestTupleSerialization extends BaseTest {

  public static enum TestEnum {
    A, B, C
  }

  ;

  public TupleMRConfig buildPangoolConfig(boolean withNullables) throws TupleMRException {
    ArrayList<Schema> schemas = new ArrayList<Schema>();
    schemas.add(new Schema("schema1",
        Fields.parse("boolean_field:boolean, int_field:int, string_field:string")));
    schemas.add(new Schema("schema2",
        Fields.parse("boolean_field:boolean, int_field:int, long_field:long")));
    schemas.add(new Schema("schema3",
        Fields.parse("boolean_field:boolean, int_field:int, long_field:long,strField:string")));
    schemas.add(new Schema("schema4",
        Fields.parse("boolean_field:boolean, int_field:int, long_field:long,strField:string")));
    schemas.add(new Schema("schema5",
        Fields.parse("boolean_field:boolean, int_field:int, long_field:long,strField:string, " +
            "enum_field:" + TestEnum.class.getName() + ",thrift_field:" + A.class.getName())));
    schemas.add(SCHEMA);
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    for (int i = 0; i < schemas.size(); i++) {
      Schema schema = schemas.get(i);
      if (withNullables) {
        schema = decorateWithNullables(schema);
      }
      b.addIntermediateSchema(schema);
    }
    b.setGroupByFields("boolean_field", "int_field");
    b.setOrderBy(new OrderBy().add("boolean_field", Order.ASC).add("int_field", Order.DESC).addSchemaOrder(Order.DESC));
    b.setSpecificOrderBy("schema1", new OrderBy().add("string_field", Order.DESC));
    b.setSpecificOrderBy("schema2", new OrderBy().add("long_field", Order.ASC));
    return b.buildConf();
  }

  @Test
  public void testRandomTupleSerialization() throws IOException, TupleMRException {
    testRandomTupleSerialization(false);
  }

  @Test
  public void testRandomTupleSerializationWithNulls() throws IOException, TupleMRException {
    testRandomTupleSerialization(true);
  }

  public void testRandomTupleSerialization(boolean withNulls) throws IOException, TupleMRException {
    Configuration conf = getConf();
    //ThriftSerialization.enableThriftSerialization(conf);

    HadoopSerialization hadoopSer = new HadoopSerialization(conf);
    //defined in BaseTest
    TupleMRConfig pangoolConf = buildPangoolConfig(withNulls);
    List<Schema> intermediateSchemas = pangoolConf.getIntermediateSchemas();

    TupleSerialization serialization = new TupleSerialization(hadoopSer, pangoolConf);

    TupleSerializer serializer = (TupleSerializer) serialization.getSerializer(null);
    TupleDeserializer deser = (TupleDeserializer) serialization.getDeserializer(null);

    int NUM_ITERATIONS = 10000;
    DatumWrapper<ITuple> wrapper = new DatumWrapper<ITuple>();
    // Different schemas
    for (int i = 0; i < NUM_ITERATIONS/2; i++) {
      Tuple tuple = new Tuple(intermediateSchemas.get(i%intermediateSchemas.size()));
      wrapper.datum(tuple);
      fillTuple(true, wrapper.datum());
      assertSerializable(serializer, deser, wrapper, false);
    }
    // Same schema
    Tuple tuple = new Tuple(pangoolConf.getIntermediateSchema("schema"));
    wrapper.datum(tuple);
    for (int i = 0; i < NUM_ITERATIONS/2; i++) {
      fillTuple(true, wrapper.datum());
      assertSerializable(serializer, deser, wrapper, false);
    }
  }

  @Test
  public void testSimpleSerializationTestWithNulls() throws IOException {
    Schema schema = new Schema("schema", Fields.parse("first:string,a:int?,b:string?"));
    Tuple t = new Tuple(schema);
    t.set(0, "first");
    t.set(1, 22);
    t.set(2, "hola");
    assertSerializable(t, false);
    t.set(1, null);
    assertSerializable(t, false);
    t.set(2, null);
    assertSerializable(t, false);
    t.set(1, 22);
    t.set(2, "hola");
    assertSerializable(t, false);

    // Now lets reuse
    Tuple re = new Tuple(schema);
    re.set(1, -1);
    re.set(2, "mal");
    t.set(1, 22);
    t.set(2, "hola");
    assertSerializable(t, re, false);
    re.set(1, -1);
    re.set(2, "mal");
    t.set(1, null);
    assertSerializable(t, re, false);
    re.set(1, -1);
    re.set(2, "mal");
    t.set(2, null);
    assertSerializable(t, re, false);
    re.set(1, -1);
    re.set(2, "mal");
    t.set(1, 22);
    t.set(2, "hola");
    assertSerializable(t, re, false);
  }
}
