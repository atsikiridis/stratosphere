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

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.operators.ReduceGroupOperator;
import eu.stratosphere.api.java.operators.UnsortedGrouping;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.hadoopcompatibility.mapred.runtime.HadoopEnvironment;
import eu.stratosphere.hadoopcompatibility.mapred.runtime.HadoopInternalJobClient;
import eu.stratosphere.hadoopcompatibility.mapred.runtime.HadoopLocalEnvironment;
import eu.stratosphere.hadoopcompatibility.mapred.runtime.HadoopLocalExecutor;
import eu.stratosphere.hadoopcompatibility.mapred.wrapper.HadoopReporter;
import eu.stratosphere.nephele.client.JobSubmissionResult;
import eu.stratosphere.util.InstantiationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.jobcontrol.Job;

import java.io.IOException;
/**
 * The user's view of Hadoop Job executed on a Stratosphere cluster.
 */
public class StratosphereHadoopJobClient  extends JobClient {

	private static final Log LOG = LogFactory.getLog(StratosphereHadoopJobClient.class);
	private static final long MAX_JOBPROFILE_AGE = 1000 * 2;
	
	private final HadoopLocalEnvironment environment;
	private HadoopInternalJobClient jobClient;

	private JobExecutionResult jobExecutionResult;
	private Configuration hadoopConf;
	HadoopLocalExecutor executor;
	private JobSubmissionResult result;


	public StratosphereHadoopJobClient(Configuration hadoopConf) {
		this(hadoopConf, (HadoopLocalEnvironment) HadoopEnvironment.getExecutionEnvironment());
	}

	public StratosphereHadoopJobClient(Configuration hadoopConf, HadoopLocalEnvironment environment) {
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

		try {
			executor = ( environment).getExecutor("TEST");
			jobClient = (HadoopInternalJobClient) executor.getJobClient();
			result = jobClient.submitJob();
		}
		catch (Exception e) {
			e.printStackTrace();
		}


		return new StratosphereRunningJob(new JobStatus());  //  TODO we need a JobStatus!
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


	/**
	 * A stratosphere job that is currently running. Based loosely on Hadoop's JobClient.NetworkedJob class.
	 */
	public class StratosphereRunningJob implements RunningJob {

		private final int DEFAULT_COMPLETION_POLL_INTERVAL = 5000;
		private final String COMPLETION_POLL_INTERVAL_KEY = "jobclient.completion.poll.inteval";

		private JobConf jobConf;
		private ExecutionEnvironment environment;
		private JobStatus status;
		private Job job;

		private int completionPollIntervalMillis;
		private long statustime;
		private JobExecutionResult jobExecutionResult;

		public StratosphereRunningJob(JobStatus status) {

			this.status = status;
			this.environment =StratosphereHadoopJobClient.this.environment;
			this.jobConf = (JobConf) StratosphereHadoopJobClient.this.getConf();
			this.statustime = System.currentTimeMillis();

			this.completionPollIntervalMillis = this.jobConf.getInt(COMPLETION_POLL_INTERVAL_KEY,
					DEFAULT_COMPLETION_POLL_INTERVAL);
			if (this.completionPollIntervalMillis < 1) {
				LOG.warn(COMPLETION_POLL_INTERVAL_KEY + " has been set to an invalid value; " +
						"replacing with  " + DEFAULT_COMPLETION_POLL_INTERVAL);
				this.completionPollIntervalMillis = this.DEFAULT_COMPLETION_POLL_INTERVAL;
			}
			try {
				job = new Job((JobConf) hadoopConf);
				job.setJobID(environment.getIdString());
			}
			catch (IOException e) {
				System.out.println("BOOM");
			}
		}

		@Override
		public JobID getID() {
			return  JobID.forName("job_200707121733_0003");  //TODO This is dummy
		}

		@Override
		public String getJobID() {
			return "job_200707121733_0003";
		}

		@Override
		public String getJobName() {
			return jobConf.getJobName();
		}

		@Override
		public String getJobFile() {
			return jobConf.getJar();
		}  // TODO this needs work.

		@Override
		public String getTrackingURL() {
			return null;
		}  // TODO as well. profile!

		@Override
		public float mapProgress() throws IOException {
			ensureFreshStatus();
			return status.mapProgress();
		}

		@Override
		public float reduceProgress() throws IOException {
			ensureFreshStatus();
			return status.reduceProgress();
		}

		@Override
		public float cleanupProgress() throws IOException {
			ensureFreshStatus();
			return status.cleanupProgress();
		}

		@Override
		public float setupProgress() throws IOException {
			ensureFreshStatus();
			return status.setupProgress();
		}

		@Override
		public boolean isComplete() throws IOException {
			updateStatus();
			final int state = getJobState();
			return (state == JobStatus.SUCCEEDED ||
					state == JobStatus.FAILED ||
					state == JobStatus.KILLED);
		}

		@Override
		public boolean isSuccessful() throws IOException {
			updateStatus();
			return getJobState() == JobStatus.SUCCEEDED;
		}

		@Override
		public void waitForCompletion() throws IOException {
			/*while (!isComplete()) {
				try {
					Thread.sleep(this.completionPollIntervalMillis);
				} catch (InterruptedException ie) {
				}
			}*/
			try {
				jobClient.waitForCompletion(result);
				executor.stop();

			}
			catch (Exception e) {
				System.out.println(e);
			}
		}

		@Override
		public int getJobState() throws IOException {
			updateStatus();
			return getJobStatus().getRunState();
		}

		@Override
		public JobStatus getJobStatus() throws IOException {
			return status;
		}

		@Override
		public void killJob() throws IOException {  //TODO Access Nephele. works in HadoopEnvironment.
			jobClient.cancelJob();
		}

		@Override
		public void setJobPriority(final String s) throws IOException {
			getJobStatus().setJobPriority(JobPriority.valueOf(s));  // ?
		}

		@Override
		public TaskCompletionEvent[] getTaskCompletionEvents(final int i) throws IOException {
			return TaskCompletionEvent.EMPTY_ARRAY;
		} // TODO must understand this better.

		@Override
		public void killTask(final TaskAttemptID taskAttemptID, final boolean b) throws IOException {
			//TODO Nephele!!! access jobclient
		}

		@Override
		public void killTask(final String s, final boolean b) throws IOException {
			TaskAttemptID taskAttemptID = TaskAttemptID.forName(s);
			killTask(taskAttemptID, b);
		}

		@Override
		public Counters getCounters() throws IOException {
			return null;//jobExecutionResult.getAllAccumulatorResults();
		}  //TODO Map accumulators to counters, almost there.

		@Override
		public String getFailureInfo() throws IOException {
			return null;
		}  //TODO

		@Override
		public String[] getTaskDiagnostics(final TaskAttemptID taskAttemptID) throws IOException {
			return new String[0];
		}

		synchronized void updateStatus() throws IOException {
			//this.status = jobSubmitClient.getJobStatus(profile.getJobID());   //TODO JobClient
			this.statustime = System.currentTimeMillis();
		}

		synchronized void ensureFreshStatus() throws IOException {
			if (System.currentTimeMillis() - statustime > MAX_JOBPROFILE_AGE) {
				updateStatus();
			}
		}
	}
}
