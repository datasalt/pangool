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
import java.util.Collection;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

import com.datasalt.pangool.io.DatumWrapper;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.serialization.HadoopSerialization;
import com.datasalt.pangool.tuplemr.TupleMRConfig;
import com.datasalt.pangool.tuplemr.TupleMRException;

/**
 * A {@link Serialization} for {@link DatumWrapper} 
 * <p>
 * To use this serialization with Hadoop, use the method {@link #enableSerialization(Configuration)} over the Hadoop
 * configuration.
 */
public class TupleSerialization implements Serialization<DatumWrapper<ITuple>>, Configurable {

	private Configuration conf;
	private com.datasalt.pangool.serialization.HadoopSerialization ser;
	private TupleMRConfig tupleMRConfig;
	
	/**
	 * Configuration parameter to enable the Schema strict validation.<br>
	 * When schema validation is set, the schema of the tuples emitted through the {@link TupleMapper} collector
	 * or {@link TupleOutputFormat} are validated, i.e. must strictly match the expected schema set
	 * in those outputs,otherwise an exception will be raised.<br><br> Using strict matching
	 * is safer, but <b>not recommended in production environment</b>, since its overhead may be great.
	 * 
	 * On the other hand,if schema validation is not set, 
	 * <a href="http://en.wikipedia.org/wiki/Duck_typing">duck typing</a>
	 *  is applied to the tuples. That is, a tuple can be accepted as long as its schema 
	 * contains all the fields from the expected schema in the same order. It may contain
	 * additional fields but these won't be serialized.
	 * 
	 * Important:<br>
	 * Schema strict validation is unset by default, and is <b>only recommended in testing environments</b>.
	 *   
	 */
	public static final String CONF_SCHEMA_VALIDATION = "pangool.schema.validation";
	
	/**
	 * see {@link #CONF_SCHEMA_VALIDATION}
	 */
	public static void enableSchemaValidation(Configuration conf){
		conf.setBoolean(CONF_SCHEMA_VALIDATION, true);
	}
	
	/**
	 * see {@link #CONF_SCHEMA_VALIDATION}
	 */
	public static void disableSchemaValidation(Configuration conf){
		conf.setBoolean(CONF_SCHEMA_VALIDATION, false);
	}
	
	/**
	 * see {@link #CONF_SCHEMA_VALIDATION}
	 */
	public static boolean getSchemaValidation(Configuration conf){
		return conf.getBoolean(CONF_SCHEMA_VALIDATION, false);
	}
	

	public TupleSerialization() {
	}
	
	public TupleSerialization(HadoopSerialization ser,TupleMRConfig tupleMRConf){
		this.ser = ser;
		this.tupleMRConfig = tupleMRConf;
	}

	@Override
	public boolean accept(Class<?> c) {
		return DatumWrapper.class.isAssignableFrom(c);
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration thatConf) {
		try {
			if(thatConf != null) {
				this.conf = new Configuration(thatConf);

				// Mega tricky!!!!. This is to avoid recursive serialization instantiation!!
				disableSerialization(this.conf);

				this.tupleMRConfig = TupleMRConfig.get(conf);
				this.ser = new com.datasalt.pangool.serialization.HadoopSerialization(this.conf);
			}
		} catch(TupleMRException e) {
			throw new RuntimeException(e);
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Serializer<DatumWrapper<ITuple>> getSerializer(Class<DatumWrapper<ITuple>> c) {
		boolean inputSchemaValidation = (conf == null ) ? true : getSchemaValidation(conf);
		return new TupleSerializer(this.ser, this.tupleMRConfig,inputSchemaValidation);
	}

	@Override
	public Deserializer<DatumWrapper<ITuple>> getDeserializer(Class<DatumWrapper<ITuple>> c) {
		return new TupleDeserializer(this.ser, this.tupleMRConfig, this.conf);
	}

	/**
	 * Use this method to enable this serialization in Hadoop
	 */
	public static void enableSerialization(Configuration conf) {
		String serClass = TupleSerialization.class.getName();
		Collection<String> currentSers = conf.getStringCollection("io.serializations");

		if(currentSers.size() == 0) {
			conf.set("io.serializations", serClass);
			return;
		}

		// Check if it is already present
		if(!currentSers.contains(serClass)) {
			currentSers.add(serClass);
			conf.setStrings("io.serializations", currentSers.toArray(new String[] {}));
		}
	}

	/**
	 * Use this method to disable this serialization in Hadoop
	 */
	public static void disableSerialization(Configuration conf) {
		String ser = conf.get("io.serializations").trim();
		String stToSearch = Pattern.quote("," + TupleSerialization.class.getName());
		ser = ser.replaceAll(stToSearch, "");
		conf.set("io.serializations", ser);
	}

}
