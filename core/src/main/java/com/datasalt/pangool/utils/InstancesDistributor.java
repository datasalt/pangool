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
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This class contains useful methods for serializing/deserializing
 * instances that implement {@link java.io.Serializable}. This class is used in Pangool
 * to distribute the instances around the cluster
 * <p>
 * You can do things like saving a Java Serializable instance and recovering it afterwards. Check methods
 * {@link InstancesDistributor#distribute(Object, String, Configuration)} and
 * {@link InstancesDistributor#loadInstance(Configuration, Class, String, boolean)} for this purpose.
 * 
 */
public class InstancesDistributor {

	public final static String HDFS_TMP_FOLDER_CONF = InstancesDistributor.class.getName() + ".hdfs.pangool.tmp.folder";

	/**
	 * Utility method for serializing an object and saving it in a way that later can be recovered
   * anywhere in the cluster.
	 * <p>
	 * The file where it has been serialized will be saved into a Hadoop Configuration property so that you can call
	 * {@link InstancesDistributor#loadInstance(Configuration, Class, String, boolean)} to re-instantiate the serialized instance.
	 * 
	 * @param obj The obj instance to serialize using Java serialization.
	 * @param fileName The file name where the instance will be serialized.
	 * @param conf The Hadoop Configuration.
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static void distribute(Object obj, String fileName, Configuration conf)
	    throws FileNotFoundException, IOException, URISyntaxException {

    FileSystem fS = FileSystem.get(conf);
		String tmpHdfsFolder = conf.get(HDFS_TMP_FOLDER_CONF);
		if(tmpHdfsFolder == null) {
			// set the temporary folder for Pangool instances to the temporary of the user that is running the Job
			// This folder will be used across the cluster for location the instances. This way, tasktrackers
			// that are being run as different user will still be able to locate this folder
			tmpHdfsFolder = conf.get("hadoop.tmp.dir");
			conf.set(HDFS_TMP_FOLDER_CONF, tmpHdfsFolder);
		}
		Path toHdfs = new Path(tmpHdfsFolder, fileName);
		if(fS.exists(toHdfs)) { // Optionally, copy to DFS if
			fS.delete(toHdfs, false);
		}

    ObjectOutput out = new ObjectOutputStream(fS.create(toHdfs));
    out.writeObject(obj);
    out.close();

		DistributedCache.addCacheFile(toHdfs.toUri(), conf);
	}

	/**
	 * Given a Hadoop Configuration property and an Class, this method can re-instantiate an Object instance that was
	 * previously distributed using	 * {@link InstancesDistributor#distribute(Object, String, Configuration)}.
	 * 
	 * @param <T>  The object type.
	 * @param conf The Hadoop Configuration.
	 * @param objClass The object type class.
	 * @param fileName The file name to locate the instance
	 * @param callSetConf If true, will call setConf() if deserialized instance is {@link Configurable}
	 * @throws IOException
	 */
	public static <T> T loadInstance(Configuration conf, Class<T> objClass, String fileName,
                                   boolean callSetConf) throws IOException {

		Path path = InstancesDistributor.locateFileInDC(conf, fileName);
		T obj;
		ObjectInput in;
		if (path == null){
			throw new IOException("Path is null");
		}
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
	 * Locates a file in the temporal folder
	 * 
	 * @param conf
	 *          The Hadoop Configuration.
	 * @param filename
	 *          The file post-fix.
	 * @throws IOException
	 */
	static Path locateFileInDC(Configuration conf, String filename) throws IOException {
      return new Path(conf.get(HDFS_TMP_FOLDER_CONF, conf.get("hadoop.tmp.dir")),
          filename);
	}

	/**
	 * The methods of this class creates some temporary files for serializing instances. This method
   * removes them. Be careful, DO NOT USE THIS METHOD unless you know what you are doing.
	 */
	public static void removeFromTemporalFolder(Configuration conf, String posfix) {
		File cacheFolder = new File(conf.get(HDFS_TMP_FOLDER_CONF, conf.get("hadoop.tmp.dir")));
		File[] files = cacheFolder.listFiles();
		for(File f : files) {
			if(f.getName().endsWith(posfix)) {
				f.delete();
			}
		}
	}
}
