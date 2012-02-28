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
package com.datasalt.pangool.benchmark.test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;
import com.google.common.io.Files;

public abstract class BaseBenchmarkTest extends AbstractHadoopTestLibrary{

	public String getReducerOutputAsText(String outputDir) throws IOException {
		return getOutputAsText(outputDir + "/part-r-00000");
	}
	
	public String getOutputAsText(String outFile) throws IOException {
		return Files.toString(new File(outFile), Charset.forName("UTF-8"));
	}
	
}
