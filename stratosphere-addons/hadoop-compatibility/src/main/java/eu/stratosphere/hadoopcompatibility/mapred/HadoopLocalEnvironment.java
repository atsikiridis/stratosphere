package eu.stratosphere.hadoopcompatibility.mapred;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.PlanExecutor;
import eu.stratosphere.api.java.LocalEnvironment;
import eu.stratosphere.client.minicluster.NepheleMiniCluster;
import org.apache.commons.lang3.reflect.FieldUtils;

public class HadoopLocalEnvironment extends LocalEnvironment {


	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		final Plan p = createProgramPlan(jobName);
		p.setDefaultParallelism(getDegreeOfParallelism());
		registerCachedFilesWithPlan(p);

		PlanExecutor executor = PlanExecutor.createLocalExecutor();
		final JobExecutionResult result = executor.executePlan(p);

		final NepheleMiniCluster cluster = (NepheleMiniCluster) FieldUtils.getDeclaredField(NepheleMiniCluster.class,
				"nephele", true).get(executor);
		cluster.getJobClient(cluster)
		initLogging();
		return
	}

}
