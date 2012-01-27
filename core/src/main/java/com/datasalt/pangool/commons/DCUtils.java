package com.datasalt.pangool.commons;

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
import java.net.URI;
import java.net.URISyntaxException;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

/**
 * This class contains useful methods for dealing with the Hadoop DistributedCache.
 * <p>
 * You can do things like saving a Java Serializable instance and recovering it afterwards. Check methods
 * {@link #serializeToDC(Serializable, String, String, Configuration)} and
 * {@link #loadSerializedObjectInDC(Configuration, Class, String)} for this purpose.
 * 
 * @author pere
 * 
 */
public class DCUtils {

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
	 * @param dcConfigurationProperty
	 *          (Optional) The Hadoop Configuration property that we will use to locate the instance in the Distributed
	 *          Cache later on. May be null if you don't want to do this here.
	 * @param conf
	 *          The Hadoop Configuration.
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static void serializeToDC(Serializable obj, String serializeToLocalFile,
	    @Nullable String dcConfigurationProperty, Configuration conf) throws FileNotFoundException, IOException,
	    URISyntaxException {
		File file = new File(serializeToLocalFile);
		ObjectOutput out = new ObjectOutputStream(new FileOutputStream(file));
		out.writeObject(obj);
		out.close();

		FileSystem fS = FileSystem.get(conf);

		Path toHdfs = new Path(file.toURI());
		if(!fS.equals(FileSystem.getLocal(conf))) { // Warning: if fs is local file is not removed
			if(fS.exists(toHdfs)) { // Optionally, copy to DFS if
				fS.delete(toHdfs, true);
			}
			FileUtil.copy(FileSystem.getLocal(conf), toHdfs, FileSystem.get(conf), toHdfs, true, conf);
		}

		if(dcConfigurationProperty != null) {
			conf.set(dcConfigurationProperty, file + "");
		}
		DistributedCache.addCacheFile(new URI(serializeToLocalFile), conf);
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
	 * @param propertyInConf
	 *          The property in the Hadoop Configuration that was used to indentify this instance.
	 * @return
	 * @throws IOException
	 */
	public static <T> T loadSerializedObjectInDC(Configuration conf, Class<T> objClass, String propertyInConf)
	    throws IOException {
		Path path = DCUtils.locateFileInDC(conf, conf.get(propertyInConf));
		ObjectInput in = new ObjectInputStream(new FileInputStream(new File(path + "")));
		T obj;
		try {
			obj = objClass.cast(in.readObject());
		} catch(ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		in.close();
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
}
