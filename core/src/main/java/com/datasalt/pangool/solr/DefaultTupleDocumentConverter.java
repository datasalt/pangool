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
package com.datasalt.pangool.solr;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.solr.common.SolrInputDocument;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;

/**
 * This default {@link TupleDocumentConverter} maps a {@link ITuple} to a SolrInputDocument using primitive
 * {@link Field} types.
 */
@SuppressWarnings("serial")
public class DefaultTupleDocumentConverter implements TupleDocumentConverter {

	public SolrInputDocument convert(ITuple key, NullWritable value) throws IOException {
		SolrInputDocument document = new SolrInputDocument();
		for(Field field : key.getSchema().getFields()) {
			checkFieldType(field);
			if(field.getType().equals(Type.STRING)) {
				// deep copy string
				document.setField(field.getName(), key.get(field.getName()).toString());
			} else { // primitive type
				document.setField(field.getName(), key.get(field.getName()));
			}
		}
		return document;
	}

	public void checkFieldType(Field field) throws IOException {
		if(field.getType().equals(Type.INT)) {
			return;
		} else if(field.getType().equals(Type.LONG)) {
			return;
		} else if(field.getType().equals(Type.STRING)) {
			return;
		} else if(field.getType().equals(Type.DOUBLE)) {
			return;
		} else if(field.getType().equals(Type.FLOAT)) {
			return;
		} else if(field.getType().equals(Type.BOOLEAN)) {
			return;
		} else {
			throw new IOException("Field type: " + field.getType() + " not supported for Tuple SOLR indexing!");
		}
	}
}
