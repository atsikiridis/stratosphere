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
import eu.stratosphere.api.java.operators.Grouping;
import eu.stratosphere.api.java.operators.ReduceGroupOperator;
import eu.stratosphere.api.java.operators.UnsortedGrouping;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.util.InstantiationUtil;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapred.lib.TokenCountMapper;

import java.io.IOException;
import java.lang.reflect.Type;


/**
 * The user's view of Hadoop Job executed on a Stratosphere cluster.
 */
public class StratosphereHadoopJobClient implements Configurable {

	private JobConf  hadoopJobConf;

	public StratosphereHadoopJobClient() {
		this.hadoopJobConf = new JobConf();
	}

	public StratosphereHadoopJobClient(JobConf hadoopJobConf) {
		this.hadoopJobConf = hadoopJobConf;
	}

	/**
	 * Submits a Hadoop job to Stratoshere (as described by the JobConf and returns after the job has been completed.
	 */
	public static void runJob(JobConf hadoopJobConf) throws Exception{
		final StratosphereHadoopJobClient jobClient = new StratosphereHadoopJobClient(hadoopJobConf);
		jobClient.submitJob(hadoopJobConf);
	}

	/**
	 * Submits a job to Stratosphere and returns a RunningJob instance which can be scheduled and monitored
	 * without blocking by default. Use waitForCompletion() to block until the job is finished.
	 */
	public void submitJob(JobConf hadoopJobConf) throws Exception{ //TODO should return a running job...
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		final DataSet input = env.createInput(getHadoopInputFormat(hadoopJobConf));
		env.setDegreeOfParallelism(1);

		final Mapper mapper = InstantiationUtil.instantiate(hadoopJobConf.getMapperClass());
		final Class mapOutputKeyClass = hadoopJobConf.getMapOutputKeyClass();
		final Class mapOutputValueClass = hadoopJobConf.getMapOutputValueClass();
		final DataSet mapped = input.flatMap(new HadoopMapFunction(mapper, mapOutputKeyClass, mapOutputValueClass));

		final UnsortedGrouping grouping = mapped.groupBy(0);

		final ReduceGroupOperator combineOp;
		final Class<? extends Reducer> combinerClass = hadoopJobConf.getCombinerClass();

		final ReduceGroupOperator reduceOp;
		final Class<? extends Reducer> reducerClass = hadoopJobConf.getReducerClass();
		final Class outputKeyClass = hadoopJobConf.getOutputKeyClass();
		final Class outputValueClass = hadoopJobConf.getOutputValueClass();
		final Reducer reducer = InstantiationUtil.instantiate(reducerClass);

		if (combinerClass != null && combinerClass.equals(reducerClass)) {
			reduceOp = grouping.reduceGroup(new HadoopReduceFunction(reducer, mapOutputKeyClass, mapOutputValueClass));
			reduceOp.setCombinable(true);
		}
		else if(combinerClass != null) {
			final Reducer combiner = InstantiationUtil.instantiate(combinerClass);
			combineOp = grouping.reduceGroup(new HadoopReduceFunction(combiner, mapOutputKeyClass, mapOutputValueClass));
			combineOp.setCombinable(true);
			reduceOp = combineOp.reduceGroup(new HadoopReduceFunction(reducer, outputKeyClass, outputValueClass));
		}
		else {
			reduceOp = grouping.reduceGroup(new HadoopReduceFunction(reducer, outputKeyClass, outputValueClass));
		}



		final HadoopOutputFormat outputFormat = new HadoopOutputFormat(hadoopJobConf.getOutputFormat() ,hadoopJobConf);
		reduceOp.output(outputFormat);

		env.execute(hadoopJobConf.getJobName());
	}

	@SuppressWarnings("unchecked")
	private HadoopInputFormat getHadoopInputFormat(JobConf jobConf) {
		final InputFormat inputFormat = jobConf.getInputFormat();
		final Class inputFormatClass = inputFormat.getClass();
		final Class inputFormatSuperClass = inputFormatClass.getSuperclass();  // What if IDENTITY super class not generic? TODO

		final Type keyType  = TypeExtractor.getParameterType(inputFormatSuperClass, inputFormatClass, 0);
		final Class keyClass = (Class) keyType;

		final Type valueType  = TypeExtractor.getParameterType(inputFormatSuperClass, inputFormatClass, 1);
		final Class valueClass = (Class) valueType;

		return new HadoopInputFormat(inputFormat, keyClass, valueClass, jobConf);
	}

	@Override
	public void setConf(Configuration configuration) {
		this.hadoopJobConf = (JobConf) configuration;
	}

	@Override
	public Configuration getConf() {
		return this.hadoopJobConf;
	}
}
