package eu.stratosphere.hadoopcompatibility.mapred;

import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.LocalEnvironment;

/**
 * Created by artem on 6/9/14.
 */
public abstract class HadoopExecutionEnvironment extends ExecutionEnvironment {

	public static ExecutionEnvironment getExecutionEnvironment() {
		return createLocalEnvironment();  //TODO contextEnvironment not supported yet.
	}

	public static LocalEnvironment createLocalEnvironment() {
		return createLocalEnvironment(getDegreeOfParallelism());
	}

	/**
	 * Creates a {@link LocalEnvironment}. The local execution environment will run the program in a
	 * multi-threaded fashion in the same JVM as the environment was created in. It will use the
	 * degree of parallelism specified in the parameter.
	 *
	 * @param degreeOfParallelism The degree of parallelism for the local environment.
	 * @return A local execution environment with the specified degree of parallelism.
	 */
	public static LocalEnvironment createLocalEnvironment(int degreeOfParallelism) {
		LocalEnvironment lee = new HadoopLocalEnvironment();
		lee.setDegreeOfParallelism(degreeOfParallelism);
		return lee;
	}

}
