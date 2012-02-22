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
package com.datasalt.pangool.benchmark.cogroup;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Identity;
import cascading.operation.regex.RegexParser;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.cogroup.RightJoin;
import cascading.scheme.Scheme;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Code for solving the URL Resolution CoGroup Problem in Cascading. 
 * <p>
 * The URL Resolution CoGroup Problem is: We have one file with URL Registers: {url	timestamp	ip} and another file with canonical
 * URL mapping: {url	canonicalUrl}. We want to obtain the URL Registers file with the url substituted with the canonical one
 * according to the mapping file: {cannonicalUrl	timestamp	ip}.
 */
public class CascadingUrlResolution {

	public final static void main(String[] args) {
		String urlMappingFile = args[0];
		String urlRegisterFile = args[1];
		String outputPath = args[2];

		final String URL_MAPPING_PIPE = "urlMapping";
		final String URL_REGISTER_PIPE = "urlRegister";

		Scheme urlMappingScheme = new TextLine(new Fields("line"));
		Tap urlMappingSource = new Hfs(urlMappingScheme, urlMappingFile);

		Scheme urlContentScheme = new TextLine(new Fields("line"));
		Tap urlRegisterSource = new Hfs(urlContentScheme, urlRegisterFile);

		Map<String, Tap> sources = new HashMap<String, Tap>();
		sources.put(URL_MAPPING_PIPE, urlMappingSource);
		sources.put(URL_REGISTER_PIPE, urlRegisterSource);

		Pipe urlMappingPipe = new Pipe(URL_MAPPING_PIPE);
		Pipe urlRegisterPipe = new Pipe(URL_REGISTER_PIPE);

		urlMappingPipe = new Each(urlMappingPipe, new Fields("line"), new RegexParser(
		    new Fields("urlMap", "cannonicalUrl"), "([^ ]*)\t([^ ]*)", new int[] { 1, 2 }));
		urlRegisterPipe = new Each(urlRegisterPipe, new Fields("line"), new RegexParser(new Fields("urlReg", "timestamp",
		    "ip"), "([^ ]*)\t([^ ]*)\t([^ ]*)", new int[] { 1, 2, 3 }));

		Pipe mergedPipe = new CoGroup(urlMappingPipe, new Fields("urlMap"), urlRegisterPipe, new Fields("urlReg"),
		    new RightJoin());
		mergedPipe = new Each(mergedPipe, new Fields("cannonicalUrl", "timestamp", "ip"), new Identity());

		// initialize app properties, tell Hadoop which jar file to use
		Properties properties = new Properties();
		FlowConnector.setApplicationJarClass(properties, PangoolUrlResolution.class);

		// plan a new Flow from the assembly using the source and sink Taps
		// with the above properties
		FlowConnector flowConnector = new FlowConnector(properties);

		Scheme sinkScheme = new TextLine(new Fields("outLine"));
		Tap sink = new Hfs(sinkScheme, outputPath, SinkMode.REPLACE);

		Flow flow = flowConnector.connect("urlresolution", sources, sink, mergedPipe);

		// execute the flow, block until complete
		flow.complete();
	}
}
