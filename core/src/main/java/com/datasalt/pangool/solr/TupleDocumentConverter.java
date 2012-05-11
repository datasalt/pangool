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
import java.io.Serializable;

import org.apache.hadoop.io.NullWritable;
import org.apache.solr.common.SolrInputDocument;

import com.datasalt.pangool.io.ITuple;

/**
 * Implementations will map {@link ITuple} to SolrInputDocument. Used by {@link TupleSolrOutputFormat}
 */
public interface TupleDocumentConverter extends Serializable {

	public SolrInputDocument convert(ITuple key, NullWritable value) throws IOException;
}
