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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains useful methods for dealing with the Hadoop DistributedCache.
 * <p>
 * You can do things like saving a Java Serializable instance and recovering it afterwards. Check methods
 * {@link #serializeToDC(Serializable, String, String, Configuration)} and
 * {@link #loadSerializedObjectInDC(Configuration, Class, String)} for this purpose.
 * 
 */
public class DCUtils {

	private static Logger log = LoggerFactory.getLogger(DCUtils.class);

	static final String TMP_FOLDER = "Pangool_instances_cache";
	
	private static File ensureTmpFolder() {
		File tmpFolder = new File(System.getProperty("java.io.tmpdir"), TMP_FOLDER);
		if (!tmpFolder.exists()) {
			tmpFolder.mkdir();
		}
		return tmpFolder;
	}
	
	/**
	 * Utility method for serializing an object and saving it in the Distributed Cache.
	 * <p>
	 * The file where it has been serialized will be saved into a Hadoop Configuration property so that you can call
	 * {@link #loadSerializedObjectInDC(Configuration, Class, String)} to re-instantiate the serialized instance.
	 * 
	 * @param obj
	 *          The obj instance to serialize using Java serialization.
	 * @param serializeToLocalFile
	 *          The local file where the instance will be serialized. It will be copied to the HDFS and removed.
	 * @param conf
	 *          The Hadoop Configuration.
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static void serializeToDC(Object obj, String serializeToLocalFile, Configuration conf) throws FileNotFoundException, IOException,
	    URISyntaxException {
		File file = new File(ensureTmpFolder(), serializeToLocalFile); 
		file.deleteOnExit();
		ObjectOutput out = new ObjectOutputStream(new FileOutputStream(file));
		out.writeObject(obj);
		out.close();

		FileSystem fS = FileSystem.get(conf);

		Path toHdfs = new Path(conf.get("hadoop.tmp.dir"), serializeToLocalFile);
		if(!fS.equals(FileSystem.getLocal(conf))) { // Warning: if fs is local file is not removed
			if(fS.exists(toHdfs)) { // Optionally, copy to DFS if
				fS.delete(toHdfs, true);
			}
			log.debug("Copying local file: " + file + " to " + toHdfs);
			FileUtil.copy(FileSystem.getLocal(conf), new Path(file + ""), FileSystem.get(conf), toHdfs, true, conf);
			DistributedCache.addCacheFile(toHdfs.toUri(), conf);
		} else {
			DistributedCache.addCacheFile(file.toURI(), conf);
		}
	}

	/**
	 * Given a Hadoop Configuration property and an Class, this method can re-instantiate an Object instance that was
	 * previously serialized and saved in the Distributed Cache like in
	 * {@link #serializeToDC(Serializable, String, String, Configuration)}.
	 * 
	 * @param <T>
	 *          The object type.
	 * @param conf
	 *          The Hadoop Configuration.
	 * @param objClass
	 *          The object type class.
	 * @param fileName
	 *          The file name to locate in DC
	 * @return
	 * @throws IOException
	 */
	public static <T> T loadSerializedObjectInDC(Configuration conf, Class<T> objClass, String fileName)
	    throws IOException {
		
		log.debug("[profile] Locate file in DC");
		Path path = DCUtils.locateFileInDC(conf, fileName);
		
		/*
		 * The following trick is for having the serialization, deserialization
		 * working on testing environments. We know files are 
		 */
		if (path == null) {
			String tmpdir = ensureTmpFolder().toString();
			log.debug("[profile] Not found in DC. Looking in " + tmpdir + " folder");
			path = locateFileInFolder(tmpdir, fileName);
		}
		
		log.debug("[profile] Deserialize instance");
		ObjectInput in = new ObjectInputStream(new FileInputStream(new File(path + "")));
		T obj;
		try {
			obj = objClass.cast(in.readObject());
		} catch(ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		in.close();
		log.debug("[profile] Done deserializing.");
		return obj;
	}
	
	static private Path locateFileInFolder(String folder, String fileStr) {
		File[] files = new File(folder).listFiles();
		for(File file: files) {
			if (file.getName().equals(fileStr)) {
				return new Path(folder, fileStr);
			}
		}
		return null;
	}

	/**
	 * Given a file post-fix, locate a file in the DistributedCache. It iterates over all the local files and returns the
	 * first one that meets this condition.
	 * 
	 * @param conf
	 *          The Hadoop Configuration.
	 * @param filePostFix
	 *          The file post-fix.
	 * @return
	 * @throws IOException
	 */
	public static Path locateFileInDC(Configuration conf, String filePostFix) throws IOException {
		Path locatedFile = null;
		Path[] paths = DistributedCache.getLocalCacheFiles(conf);
		if(paths == null) {
			return null;
		}
		for(Path p : paths) {
			if(p.toString().endsWith(filePostFix)) {
				locatedFile = p;
				break;
			}
		}
		return locatedFile;
	}
	
	/**
	 * The methods of this class creates some temporary files
	 * for serializing instances. This method removes them. 
	 */
	public static void cleanupTemporaryInstanceCache() {
		File cacheFolder = ensureTmpFolder();
		File[] files = cacheFolder.listFiles();
		for (File f: files) {
			f.delete();
		}
	}
}
