package org.apache.hadoop.mapred;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobSubmissionProtocol;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

public class Sandbox {

	@Test
	public void test() throws IOException {
		Configuration configuration = new Configuration();
		JobConf conf = new JobConf(configuration);
		JobSubmissionProtocol jobSubmitClient;
		String tracker = conf.get("mapred.job.tracker", "local");
		if("local".equals(tracker)) {
			conf.setNumMapTasks(1);
			jobSubmitClient = new LocalJobRunner(conf);
		} else {
			jobSubmitClient = createRPCProxy(JobTracker.getAddress(conf), conf);
		}
		JobID jobId = jobSubmitClient.getNewJobId();
		System.out.println(jobId);
	}

	private static JobSubmissionProtocol createRPCProxy(InetSocketAddress addr, Configuration conf) throws IOException {
		return (JobSubmissionProtocol) RPC.getProxy(JobSubmissionProtocol.class, JobSubmissionProtocol.versionID, addr,
		    UserGroupInformation.getCurrentUser(), conf, NetUtils.getSocketFactory(conf, JobSubmissionProtocol.class));
	}
}
