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
package com.datasalt.pangool.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;

import com.datasalt.pangool.serialization.protostuff.ProtoStuffSerialization;
import com.datasalt.pangool.serialization.thrift.ThriftSerialization;
import com.datasalt.pangool.serialization.tuples.PangoolSerialization;

public abstract class AbstractBaseTest {
	
	private Configuration conf;

	public Configuration getConf() throws IOException {
		if (conf == null){
			conf =createConf();
		}
		return conf;
	}

	public static Configuration createNewConfiguration() {
		Configuration conf = new Configuration();
		return conf;
	}
	
	private static void configureSerialization(Configuration conf) {
		ThriftSerialization.enableThriftSerialization(conf);
		ProtoStuffSerialization.enableProtoStuffSerialization(conf);		
		PangoolSerialization.enableSerialization(conf);
	}
	
	private Configuration createConf(){
		Configuration conf = createNewConfiguration();
		configureSerialization(conf);
		return conf;
	}
	
	@Before
	public void prepare() throws IOException {

	}
}
