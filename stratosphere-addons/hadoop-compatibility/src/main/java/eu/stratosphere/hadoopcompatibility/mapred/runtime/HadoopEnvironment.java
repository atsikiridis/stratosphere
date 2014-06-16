package eu.stratosphere.hadoopcompatibility.mapred.runtime;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.PlanExecutor;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.LocalEnvironment;

/**
 * Created by artem on 6/14/14.
 */
public abstract class HadoopEnvironment extends ExecutionEnvironment {

	private static ExecutionEnvironment contextEnvironment;

	private static int defaultLocalDop = Runtime.getRuntime().availableProcessors();

	public static ExecutionEnvironment getExecutionEnvironment() {
		return contextEnvironment == null ? createLocalEnvironment() : contextEnvironment;
	}
	public static LocalEnvironment createLocalEnvironment() {
		return createLocalEnvironment(defaultLocalDop);
	}

	public static LocalEnvironment createLocalEnvironment(int degreeOfParallelism) {
		LocalEnvironment lee = new HadoopLocalEnvironment();
		lee.setDegreeOfParallelism(degreeOfParallelism);
		return lee;
	}



}
