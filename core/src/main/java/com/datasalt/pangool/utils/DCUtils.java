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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

/**
 * This class contains useful methods for dealing with the Hadoop DistributedCache.
 * <p>
 * You can do things like saving a Java Serializable instance and recovering it afterwards. Check methods
 * {@link DCUtils#serializeToDC(Object, String, Configuration)} and
 * {@link DCUtils#loadSerializedObjectInDC(Configuration, Class, String, boolean)} for this purpose.
 * 
 */
public class DCUtils {

	public final static String HDFS_TMP_FOLDER_CONF = DCUtils.class.getName() + ".hdfs.pangool.tmp.folder";

	/**
	 * Utility method for serializing an object and saving it in the Distributed Cache.
	 * <p>
	 * The file where it has been serialized will be saved into a Hadoop Configuration property so that you can call
	 * {@link DCUtils#loadSerializedObjectInDC(Configuration, Class, String, boolean)} to re-instantiate the serialized instance.
	 * 
	 * @param obj The obj instance to serialize using Java serialization.
	 * @param serializeToLocalFile The local file where the instance will be serialized. It will be copied to the HDFS and removed.
	 * @param conf The Hadoop Configuration.
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static void serializeToDC(Object obj, String serializeToLocalFile, Configuration conf)
	    throws FileNotFoundException, IOException, URISyntaxException {

		File hadoopTmpDir = new File(conf.get("hadoop.tmp.dir"));
		if(!hadoopTmpDir.exists()) {
			hadoopTmpDir.mkdir();
		}
		File file = new File(hadoopTmpDir, serializeToLocalFile);
		FileSystem fS = FileSystem.get(conf);

		ObjectOutput out = new ObjectOutputStream(new FileOutputStream(file));
		out.writeObject(obj);
		out.close();

		if(fS.equals(FileSystem.getLocal(conf))) {
			return;
		}

		String tmpHdfsFolder = conf.get(HDFS_TMP_FOLDER_CONF);
		if(tmpHdfsFolder == null) {
			// set the temporary folder for Pangool instances to the temporary of the user that is running the Job
			// This folder will be used across the cluster for location the instances. This way, tasktrackers
			// that are being run as different user will still be able to locate this folder
			tmpHdfsFolder = conf.get("hadoop.tmp.dir");
			conf.set(HDFS_TMP_FOLDER_CONF, tmpHdfsFolder);
		}
		Path toHdfs = new Path(tmpHdfsFolder, serializeToLocalFile);
		if(fS.exists(toHdfs)) { // Optionally, copy to DFS if
			fS.delete(toHdfs, false);
		}
		FileUtil.copy(FileSystem.getLocal(conf), new Path(file + ""), FileSystem.get(conf), toHdfs, true, conf);
		DistributedCache.addCacheFile(toHdfs.toUri(), conf);
	}

	/**
	 * Given a Hadoop Configuration property and an Class, this method can re-instantiate an Object instance that was
	 * previously serialized and saved in the Distributed Cache like in
	 * {@link DCUtils#serializeToDC(Object, String, Configuration)}.
	 * 
	 * @param <T>  The object type.
	 * @param conf The Hadoop Configuration.
	 * @param objClass The object type class.
	 * @param fileName The file name to locate in DC
	 * @param callSetConf If true, will call setConf() if deserialized object is Configurable
	 * @throws IOException
	 */
	public static <T> T loadSerializedObjectInDC(Configuration conf, Class<T> objClass, String fileName,
	    boolean callSetConf) throws IOException {

		Path path = DCUtils.locateFileInDC(conf, fileName);
		T obj;
		ObjectInput in;

		in = new ObjectInputStream(FileSystem.get(conf).open(path));

		try {
			obj = objClass.cast(in.readObject());
		} catch(ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		in.close();
		if(obj instanceof Configurable && callSetConf) {
			((Configurable) obj).setConf(conf);
		}
		return obj;
	}

	/**
	 * Given a file post-fix, locate a file in the DistributedCache. It iterates over all the local files and returns the
	 * first one that meets this condition.
	 * 
	 * @param conf
	 *          The Hadoop Configuration.
	 * @param filePostFix
	 *          The file post-fix.
	 * @throws IOException
	 */
	public static Path locateFileInDC(Configuration conf, String filePostFix) throws IOException {
		FileSystem fS = FileSystem.get(conf);
		Path locatedFile = null;

		if(fS.equals(FileSystem.getLocal(conf))) {
			// We use the File Java API in local because the Hadoop Path, FileSystem, etc is too slow for tests that
			// need to call this method a lot
			File tmpFolder = new File(conf.get("hadoop.tmp.dir"));
			for(File file : tmpFolder.listFiles()) {
				if(file.getName().endsWith(filePostFix)) {
					locatedFile = new Path(file.toString());
					break;
				}
			}
		} else {
			Path tmpHdfsFolder = new Path(conf.get(HDFS_TMP_FOLDER_CONF, conf.get("hadoop.tmp.dir")));
			for(FileStatus fSt : fS.listStatus(tmpHdfsFolder)) {
				Path path = fSt.getPath();
				if(path.toString().endsWith(filePostFix)) {
					locatedFile = path;
					break;
				}
			}
		}

		return locatedFile;
	}

	/**
	 * The methods of this class creates some temporary files for serializing instances. This method removes them.
	 */
	public static void cleanupTemporaryInstanceCache(Configuration conf, String prefix) {
		File cacheFolder = new File(conf.get("hadoop.tmp.dir"));
		File[] files = cacheFolder.listFiles();
		for(File f : files) {
			if(f.getName().endsWith(prefix)) {
				f.delete();
			}
		}
	}
}
