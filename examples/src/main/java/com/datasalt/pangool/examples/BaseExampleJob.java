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
package com.datasalt.pangool.examples;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public abstract class BaseExampleJob implements Tool, Configurable {

	public final static Charset UTF8 = Charset.forName("UTF-8");
	private String help;
	protected Configuration conf;

	public BaseExampleJob() {
		this("[no help specified]");
	}

	public BaseExampleJob(String help) {
		this.help = help;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	public void failArguments(String message) {
		System.err.println(message);
		System.err.println();
		ToolRunner.printGenericCommandUsage(System.out);
		System.out.println(help);
	}

	public void delete(String output) throws IOException {
		FileSystem.get(conf).delete(new Path(output), true);
	}
}
