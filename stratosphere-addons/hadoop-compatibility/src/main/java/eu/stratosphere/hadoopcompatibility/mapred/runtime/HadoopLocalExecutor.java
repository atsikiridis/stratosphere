package eu.stratosphere.hadoopcompatibility.mapred.runtime;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.contextcheck.ContextChecker;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import org.apache.commons.lang3.reflect.FieldUtils;

/**
 * Created by artem on 6/14/14.
 */
public class HadoopLocalExecutor extends LocalExecutor {

	private HadoopInternalJobClient jobClient;
	private boolean shutDownAtEnd;


	public void prepareExecution(Plan plan) throws Exception {
		System.out.println("YOOO");
		//NepheleMiniCluster nephele = (NepheleMiniCluster) FieldUtils.getDeclaredField(this.getClass(),"nephele", true).get(this);
		if (plan == null) {
			throw new IllegalArgumentException("The plan may not be null.");
		}

		ContextChecker checker = new ContextChecker();
		checker.check(plan);

		synchronized (this.lock) {

			// check if we start a session dedicated for this execution
			if (this.nephele == null) {
				// we start a session just for us now
				shutDownAtEnd = true;
				this.start();
			} else {
				// we use the existing session
				shutDownAtEnd = false;
			}


			PactCompiler pc = new PactCompiler(new DataStatistics());
			OptimizedPlan op = pc.compile(plan);

			NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
			JobGraph jobGraph = jgg.compileJobGraph(op);

			jobClient = (HadoopInternalJobClient) this.nephele.getJobClient(jobGraph); //TODO Or maybe return this.nephele?

		}
	}

	public void start() throws Exception {
		synchronized (this.lock) {
			if (this.nephele == null) {

				// create the embedded runtime
				this.nephele = new HadoopNepheleMiniCluster();

				// configure it, if values were changed. otherwise the embedded runtime uses the internal defaults
				if (jobManagerRpcPort > 0) {
					nephele.setJobManagerRpcPort(jobManagerRpcPort);
				}
				if (taskManagerRpcPort > 0) {
					nephele.setTaskManagerRpcPort(jobManagerRpcPort);
				}
				if (taskManagerDataPort > 0) {
					nephele.setTaskManagerDataPort(taskManagerDataPort);
				}
				if (configDir != null) {
					nephele.setConfigDir(configDir);
				}
				if (hdfsConfigFile != null) {
					nephele.setHdfsConfigFile(hdfsConfigFile);
				}
				nephele.setDefaultOverwriteFiles(defaultOverwriteFiles);
				nephele.setDefaultAlwaysCreateDirectory(defaultAlwaysCreateDirectory);

				// start it up
				this.nephele.start();
			} else {
				throw new IllegalStateException("The local executor was already started.");
			}
		}
	}




	public JobClient getJobClient() {
		return jobClient;
	}

	public boolean shoudShutDownAtEnd() {
		return shutDownAtEnd;
	}
}
