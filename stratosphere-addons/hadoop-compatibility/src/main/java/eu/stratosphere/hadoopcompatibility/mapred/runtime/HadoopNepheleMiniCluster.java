package eu.stratosphere.hadoopcompatibility.mapred.runtime;

import eu.stratosphere.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.jobgraph.JobGraph;

/**
 * Created by artem on 6/14/14.
 */
public class HadoopNepheleMiniCluster extends NepheleMiniCluster {

	private static final int DEFAULT_JM_RPC_PORT = 6498;

	private int jobManagerRpcPort = DEFAULT_JM_RPC_PORT;

	public void setJobManagerRpcPort(int jobManagerRpcPort) {
		this.jobManagerRpcPort = jobManagerRpcPort;
	}

	public JobClient getJobClient(JobGraph jobGraph) throws Exception {
		Configuration configuration = jobGraph.getJobConfiguration();
		configuration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
		configuration.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerRpcPort);
		return new HadoopInternalJobClient(jobGraph, configuration);
	}

}
