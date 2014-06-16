package eu.stratosphere.hadoopcompatibility.mapred.runtime;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.accumulators.AccumulatorHelper;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.client.AbstractJobResult;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.client.JobProgressResult;
import eu.stratosphere.nephele.client.JobSubmissionResult;
import eu.stratosphere.nephele.event.job.AbstractEvent;
import eu.stratosphere.nephele.event.job.JobEvent;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobStatus;
import eu.stratosphere.nephele.protocols.AccumulatorProtocol;
import eu.stratosphere.nephele.protocols.JobManagementProtocol;
import eu.stratosphere.nephele.services.accumulators.AccumulatorEvent;
import eu.stratosphere.nephele.types.IntegerRecord;
import eu.stratosphere.util.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Map;

public class HadoopInternalJobClient extends JobClient {


	private static final Log LOG = LogFactory.getLog(JobClient.class);

	public HadoopInternalJobClient(final JobGraph jobGraph, Configuration configuration) throws IOException {

		super(jobGraph, configuration);
	}


	public JobExecutionResult waitForCompletion(JobSubmissionResult submissionResult) throws Exception{

		//JobManagementProtocol jobSubmitClient = (JobManagementProtocol)  FieldUtils.getDeclaredField(super.getClass(), "jobSubmitClient", true).get(this);
		//JobCleanUp jobCleanUp = (JobCleanUp)  FieldUtils.getDeclaredField(super.getClass(), "jobCleanup", true).get(this);
		//long lastProcessedEventSequenceNumber = (Long) FieldUtils.getDeclaredField(super.getClass(), "lastProcessedEventSequenceNumber", true).get(this);
		//PrintStream console = (PrintStream) FieldUtils.getDeclaredField(super.getClass(), "console", true).get(this);

		synchronized (jobSubmitClient) {

			//final JobSubmissionResult submissionResult = jobSubmitClient.submitJob(jobGraph);
			if (submissionResult.getReturnCode() == AbstractJobResult.ReturnCode.ERROR) {
				LOG.error("ERROR: " + submissionResult.getDescription());
				throw new JobExecutionException(submissionResult.getDescription(), false);
			}

			// Make sure the job is properly terminated when the user shut's down the client
			Runtime.getRuntime().addShutdownHook(jobCleanUp);
		}

		long sleep = 0;
		try {
			final IntegerRecord interval = jobSubmitClient.getRecommendedPollingInterval();
			sleep = interval.getValue() * 1000;
		} catch (IOException ioe) {
			Runtime.getRuntime().removeShutdownHook(jobCleanUp);
			// Rethrow error
			throw ioe;
		}

		try {
			Thread.sleep(sleep / 2);
		} catch (InterruptedException e) {
			Runtime.getRuntime().removeShutdownHook(jobCleanUp);
			logErrorAndRethrow(StringUtils.stringifyException(e));
		}

		long startTimestamp = -1;

		while (true) {

			if (Thread.interrupted()) {
				logErrorAndRethrow("Job client has been interrupted");
			}

			JobProgressResult jobProgressResult = null;
			try {
				jobProgressResult = getJobProgress();
			} catch (IOException ioe) {
				Runtime.getRuntime().removeShutdownHook(jobCleanUp);
				// Rethrow error
				throw ioe;
			}

			if (jobProgressResult == null) {
				logErrorAndRethrow("Returned job progress is unexpectedly null!");
			}

			if (jobProgressResult.getReturnCode() == AbstractJobResult.ReturnCode.ERROR) {
				logErrorAndRethrow("Could not retrieve job progress: " + jobProgressResult.getDescription());
			}

			final Iterator<AbstractEvent> it = jobProgressResult.getEvents();
			while (it.hasNext()) {

				final AbstractEvent event = it.next();

				// Did we already process that event?
				if (lastProcessedEventSequenceNumber >= event.getSequenceNumber()) {
					continue;
				}

				LOG.info(event.toString());
				if (console != null) {
					console.println(event.toString());
				}

				lastProcessedEventSequenceNumber = event.getSequenceNumber();

				// Check if we can exit the loop
				if (event instanceof JobEvent) {
					final JobEvent jobEvent = (JobEvent) event;
					final JobStatus jobStatus = jobEvent.getCurrentJobStatus();
					if (jobStatus == JobStatus.SCHEDULED) {
						startTimestamp = jobEvent.getTimestamp();
					}
					if (jobStatus == JobStatus.FINISHED) {
						Runtime.getRuntime().removeShutdownHook(jobCleanUp);
						final long jobDuration = jobEvent.getTimestamp() - startTimestamp;

						// Request accumulators
						Map<String, Object> accumulators = null;
						try {
							accumulators = AccumulatorHelper.toResultMap(getAccumulators().getAccumulators());
						} catch (IOException ioe) {
							Runtime.getRuntime().removeShutdownHook(jobCleanUp);
							throw ioe;	// Rethrow error
						}
						return new JobExecutionResult(jobDuration, accumulators);

					} else if (jobStatus == JobStatus.CANCELED || jobStatus == JobStatus.FAILED) {
						Runtime.getRuntime().removeShutdownHook(jobCleanUp);
						LOG.info(jobEvent.getOptionalMessage());
						if (jobStatus == JobStatus.CANCELED) {
							throw new JobExecutionException(jobEvent.getOptionalMessage(), true);
						} else {
							throw new JobExecutionException(jobEvent.getOptionalMessage(), false);
						}
					}
				}
			}

			try {
				Thread.sleep(sleep);
			} catch (InterruptedException e) {
				logErrorAndRethrow(StringUtils.stringifyException(e));
			}
		}

	}









}
