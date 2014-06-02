package eu.stratosphere.hadoopcompatibility.mapred.wrapper;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.java.ExecutionEnvironment;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import java.io.IOException;

public class StratosphereRunningJob implements RunningJob {

	private final ExecutionEnvironment env;
	private final JobConf jobConf;
	private JobExecutionResult jobExecutionResult;

	public StratosphereRunningJob(JobConf jobConf, ExecutionEnvironment env) {
		this.env =env;
		this.jobConf = jobConf;
	}

	@Override
	public JobID getID() {
		return  JobID.forName(env.getIdString());
	}

	@Override
	public String getJobID() {
		return getID().toString();
	}

	@Override
	public String getJobName() {
		return jobConf.getJobName();
	}

	@Override
	public String getJobFile() {
		return jobConf.getJar();
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
		final int state = getJobState();
		return (state == JobStatus.SUCCEEDED ||
				state == JobStatus.FAILED ||
				state == JobStatus.KILLED);
	}

	@Override
	public boolean isSuccessful() throws IOException {
		return getJobState() == JobStatus.SUCCEEDED;
	}

	@Override
	public void waitForCompletion() throws IOException {  //DUMMY until we support Counters.
		try {
		    jobExecutionResult = env.execute(getJobName());
		}
		catch (Exception e) {
			throw new IOException("An error occured running the " + getJobName() + " Hadoop Job" +
					" on Stratoshere. Exception: " + e);
		}
	}

	@Override
	public int getJobState() throws IOException {
		return getJobStatus().getRunState();
	}

	@Override
	public JobStatus getJobStatus() throws IOException {
		return new JobStatus(getID(), setupProgress(), mapProgress(), reduceProgress(), cleanupProgress(),
				getJobState(), jobConf.getJobPriority());
	}

	@Override
	public void killJob() throws IOException {  //TODO Access Nephele.

	}

	@Override
	public void setJobPriority(final String s) throws IOException {
		getJobStatus().setJobPriority(JobPriority.valueOf(s));
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
		TaskAttemptID taskAttemptID = TaskAttemptID.forName(s);
		killTask(taskAttemptID, b);
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
