/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.hadoopcompatibility.mapred;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.operators.ReduceGroupOperator;
import eu.stratosphere.api.java.operators.UnsortedGrouping;
import eu.stratosphere.hadoopcompatibility.mapred.wrapper.HadoopReporter;
import eu.stratosphere.util.InstantiationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskCompletionEvent;

import java.io.IOException;
/**
 * The user's view of Hadoop Job executed on a Stratosphere cluster.
 */
public class StratosphereHadoopJobClient  extends JobClient {

	private final ExecutionEnvironment environment;

	private Configuration hadoopConf;


	public StratosphereHadoopJobClient(Configuration hadoopConf) {
		this(hadoopConf, (ExecutionEnvironment.getExecutionEnvironment()));
	}

	public StratosphereHadoopJobClient(Configuration hadoopConf, ExecutionEnvironment environment) {
		this.hadoopConf = hadoopConf;
		this.environment = environment;
		this.environment.setDegreeOfParallelism(1); //TODO make configurable.
	}

	/**
	 * Submits a Hadoop job to Stratoshere (as described by the JobConf and returns after the job has been completed.
	 */
	public static RunningJob runJob(JobConf hadoopJobConf) throws IOException{
		final StratosphereHadoopJobClient jobClient = new StratosphereHadoopJobClient(hadoopJobConf);
		RunningJob job = jobClient.submitJob(hadoopJobConf);
		job.waitForCompletion();
		return job;

	}

	/**
	 * Submits a job to Stratosphere and returns a RunningJob instance which can be scheduled and monitored
	 * without blocking by default. Use waitForCompletion() to block until the job is finished.
	 */
	@SuppressWarnings("unchecked")
	public RunningJob submitJob(JobConf hadoopJobConf) throws IOException{

		//setting up the inputFormat for the job
		final DataSet input = environment.createInput(getStratosphereInputFormat(hadoopJobConf));

		final Mapper mapper = InstantiationUtil.instantiate(hadoopJobConf.getMapperClass());
		final Class mapOutputKeyClass = hadoopJobConf.getMapOutputKeyClass();
		final Class mapOutputValueClass = hadoopJobConf.getMapOutputValueClass();
		final DataSet mapped = input.flatMap(new HadoopMapFunction(mapper, mapOutputKeyClass, mapOutputValueClass));

		//Partitioning  TODO Custom partitioning
		final UnsortedGrouping grouping = mapped.groupBy(0);

		//Is a combiner specified in the jobConf?
		final Class<? extends Reducer> combinerClass = hadoopJobConf.getCombinerClass();

		//Is a Reducer specified ? No reducer means identity reducer.
		final Class<? extends Reducer> reducerClass = hadoopJobConf.getReducerClass();
		final Reducer reducer = InstantiationUtil.instantiate(reducerClass);

		//The output types of the reducers.
		final Class outputKeyClass = hadoopJobConf.getOutputKeyClass();
		final Class outputValueClass = hadoopJobConf.getOutputValueClass();

		final ReduceGroupOperator reduceOp;
		if (combinerClass != null && combinerClass.equals(reducerClass)) {
			reduceOp = grouping.reduceGroup(new HadoopReduceFunction(reducer, mapOutputKeyClass, mapOutputValueClass));
			reduceOp.setCombinable(true);  //The combiner is the same class as the reducer.
		}
		else if(combinerClass != null) {  //We have a different combiner.
			final Reducer combiner = InstantiationUtil.instantiate(combinerClass);
			final ReduceGroupOperator combineOp = grouping.reduceGroup(new HadoopReduceFunction(combiner,
					mapOutputKeyClass, mapOutputValueClass));
			combineOp.setCombinable(true);
			final HadoopReduceFunction reduceFunction = new HadoopReduceFunction(reducer, outputKeyClass,
					outputValueClass);
			reduceOp = combineOp.groupBy(0).reduceGroup(reduceFunction);
		}
		else { // No combiner.
			reduceOp = grouping.reduceGroup(new HadoopReduceFunction(reducer, outputKeyClass, outputValueClass));
		}

		//Wrapping the output format.
		final HadoopOutputFormat outputFormat = new HadoopOutputFormat(hadoopJobConf.getOutputFormat() ,hadoopJobConf);
		reduceOp.output(outputFormat);

		return new DummyStratosphereRunningJob(environment, hadoopJobConf.getJobName());
	}


	@SuppressWarnings("unchecked")
	private HadoopInputFormat getStratosphereInputFormat(JobConf jobConf) throws IOException{
		final InputFormat inputFormat = jobConf.getInputFormat();
		final Class[] inputFormatClasses = getInputFormatClasses(inputFormat, jobConf);
		return new HadoopInputFormat(inputFormat, inputFormatClasses[0], inputFormatClasses[1], jobConf);
	}

	/**
	 * Better... Still not always.
	 */
	private Class[] getInputFormatClasses(InputFormat inputFormat, JobConf jobConf) throws IOException{
		final Class[] inputFormatClasses = new Class[2];
		final InputSplit firstSplit = inputFormat.getSplits(jobConf, 0)[0];
		final Reporter reporter = new HadoopReporter();
		inputFormatClasses[0] = inputFormat.getRecordReader(firstSplit, jobConf, reporter).createKey().getClass();
		inputFormatClasses[1] = inputFormat.getRecordReader(firstSplit, jobConf, reporter).createValue().getClass();
		return inputFormatClasses;
	}
	
	@Override
	public void setConf(Configuration conf) {
		this.hadoopConf = conf;
	}
	
	@Override
	public Configuration getConf() {
		return this.hadoopConf;
	}

	private class DummyStratosphereRunningJob implements RunningJob {

		private final ExecutionEnvironment executionEnvironment;
		private final String jobName;

		public DummyStratosphereRunningJob(ExecutionEnvironment executionEnvironment, String jobName) {
			this.executionEnvironment = executionEnvironment;
			this.jobName = jobName;
		}

		@Override
		public JobID getID() {
			return null;
		}

		@Override
		public String getJobID() {
			return null;
		}

		@Override
		public String getJobName() {
			return null;
		}

		@Override
		public String getJobFile() {
			return null;
		}

		@Override
		public String getTrackingURL() {
			return null;
		}

		@Override
		public float mapProgress() throws IOException {
			return 0;
		}

		@Override
		public float reduceProgress() throws IOException {
			return 0;
		}

		@Override
		public float cleanupProgress() throws IOException {
			return 0;
		}

		@Override
		public float setupProgress() throws IOException {
			return 0;
		}

		@Override
		public boolean isComplete() throws IOException {
			return false;
		}

		@Override
		public boolean isSuccessful() throws IOException {
			return false;
		}

		@Override
		public void waitForCompletion() throws IOException {
			try {
				this.executionEnvironment.execute(jobName);
			}
			catch (Exception e) {
				throw new IOException("An error has occurred.", e);
			}
		}

		@Override
		public int getJobState() throws IOException {
			return 0;
		}

		@Override
		public JobStatus getJobStatus() throws IOException {
			return null;
		}

		@Override
		public void killJob() throws IOException {

		}

		@Override
		public void setJobPriority(final String s) throws IOException {

		}

		@Override
		public TaskCompletionEvent[] getTaskCompletionEvents(final int i) throws IOException {
			return new TaskCompletionEvent[0];
		}

		@Override
		public void killTask(final TaskAttemptID taskAttemptID, final boolean b) throws IOException {

		}

		@Override
		public void killTask(final String s, final boolean b) throws IOException {

		}

		@Override
		public Counters getCounters() throws IOException {
			return null;
		}

		@Override
		public String getFailureInfo() throws IOException {
			return null;
		}

		@Override
		public String[] getTaskDiagnostics(final TaskAttemptID taskAttemptID) throws IOException {
			return new String[0];
		}
	}
}
