package eu.stratosphere.hadoopcompatibility.mapred.runtime;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.PlanExecutor;
import eu.stratosphere.api.java.LocalEnvironment;
import eu.stratosphere.client.LocalExecutor;

import java.io.IOException;

public class HadoopLocalEnvironment extends LocalEnvironment {

	public HadoopLocalExecutor getExecutor(String jobName) throws Exception{
		Plan p = createProgramPlan(jobName); //plan should got level up
		p.setDefaultParallelism(getDegreeOfParallelism());
		registerCachedFilesWithPlan(p);

		HadoopLocalExecutor executor = (HadoopLocalExecutor)HadoopPlanExecutor.createLocalExecutor();
		executor.prepareExecution(p);
		return  executor;
	}

}
