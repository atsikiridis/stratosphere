package eu.stratosphere.hadoopcompatibility.mapred.runtime;

import eu.stratosphere.api.common.PlanExecutor;

/**
 * Created by artem on 6/14/14.
 */
public abstract class HadoopPlanExecutor extends PlanExecutor {

	private static final String LOCAL_EXECUTOR_CLASS = "eu.stratosphere.hadoopcompatibility.mapred.runtime.HadoopLocalExecutor";
	private static final String REMOTE_EXECUTOR_CLASS = "eu.stratosphere.client.RemoteExecutor";

	public static PlanExecutor createLocalExecutor() {
		Class<? extends PlanExecutor> leClass = loadExecutorClass(LOCAL_EXECUTOR_CLASS);

		try {
			return leClass.newInstance();
		}
		catch (Throwable t) {
			throw new RuntimeException("An error occurred while loading the local executor (" + LOCAL_EXECUTOR_CLASS + ").", t);
		}
	}

	private static final Class<? extends PlanExecutor> loadExecutorClass(String className) {
		try {
			Class<?> leClass = Class.forName(className);
			return leClass.asSubclass(PlanExecutor.class);
		}
		catch (ClassNotFoundException cnfe) {
			throw new RuntimeException("Could not load the executor class (" + className + "). Do you have the 'stratosphere-clients' project in your dependencies?");
		}
		catch (Throwable t) {
			throw new RuntimeException("An error occurred while loading the executor (" + className + ").", t);
		}
	}
}
