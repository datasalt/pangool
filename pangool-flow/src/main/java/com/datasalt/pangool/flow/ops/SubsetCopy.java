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
package com.datasalt.pangool.flow.ops;

import com.datasalt.pangool.io.Schema;

/**
 * Operation that copies an input Tuple A with Schema(A) to a Tuple B with Schema(B). Schema(B) must be a subset of
 * Schema(A). This operation can be used for selecting a subset of the fields of one Tuple.
 */
@SuppressWarnings("serial")
public class SubsetCopy extends Copy {

	public SubsetCopy(Schema schema) {
		super(schema);
	}
}
