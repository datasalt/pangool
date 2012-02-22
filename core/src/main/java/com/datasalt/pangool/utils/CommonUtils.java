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
package com.datasalt.pangool.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class CommonUtils {

	@SuppressWarnings({ "rawtypes", "unchecked" })
  public static Map invertMap(Map<?,?> map){
		Map result = new TreeMap();
		for (Map.Entry entry : map.entrySet()){
  		result.put(entry.getValue(),entry.getKey());
  	}
		return result;
	}
	
	/**
	 * Writes the string into the file. 
	 */
	public static void writeTXT(String string, File file) throws IOException {
		BufferedWriter out = new BufferedWriter(new FileWriter(file));
		out.write(string);
		out.close();
	}
	
}
