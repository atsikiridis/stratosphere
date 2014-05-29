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

import com.google.common.reflect.Reflection;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.io.TextInputFormat;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.hadoopcompatibility.mapred.record.datatypes.WritableComparableWrapper;
import eu.stratosphere.types.TypeInformation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapred.lib.TokenCountMapper;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;


/**
 * The user's view of Hadoop Job executed on a Stratosphere cluster.
 */
public class StratosphereJobClient extends JobClient { //or not extend?

	private JobConf  hadoopJobConf;

	public StratosphereJobClient() {
		this.hadoopJobConf = new JobConf();
	}

	public StratosphereJobClient(JobConf hadoopJobConf) {
		this.hadoopJobConf = hadoopJobConf;
	}

	/**
	 * Submits a Hadoop job to Stratoshere (as described by the JobConf and returns after the job has been completed.
	 */
	public static RunningJob runJob(JobConf hadoopJobConf) throws IOException{
		StratosphereJobClient jobClient = new StratosphereJobClient(hadoopJobConf);
		RunningJob runningJob = jobClient.submitJob(hadoopJobConf);
		return runningJob;
	}

	/**
	 * Submits a job to Stratosphere and returns a RunningJob instance which can be scheduled and monitored
	 * without blocking by default. Use waitForCompletion() to block until the job is finished.
	 */
	@Override
	public RunningJob submitJob(JobConf hadoopJobConf) {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(1);

		HadoopInputFormat inputFormat = getHadoopInputFormat(hadoopJobConf);
		HadoopMapFunction mapFunction = getMapFunction(hadoopJobConf);

		HadoopReduceFunction<?,?,?,?> reduceFunction = getReduceFunction(hadoopJobConf);

		HadoopOutputFormat outputFormat = getOutputFormat(hadoopJobConf);

		final DataSet<Tuple2<?, ?>> text = env.createInput(inputFormat);
		DataSet mapped = text.flatMap(mapFunction);
		DataSet result = mapped.groupBy(0).reduceGroup(reduceFunction);
		result.output(outputFormat);
		try {
			env.execute("Test");
		}
		catch (Exception e) {
			System.out.println(e);
		}
		return null;
	}

	private HadoopInputFormat getHadoopInputFormat(JobConf jobConf) {
		InputFormat<?,?> inputFormat = jobConf.getInputFormat();
		Class key = (Class)((ParameterizedType) (inputFormat.getClass().getGenericSuperclass())).getActualTypeArguments()[0];
		Class value  = (Class)((ParameterizedType) (inputFormat.getClass().getGenericSuperclass())).getActualTypeArguments()[1];
		HadoopInputFormat<?,?> inputFormatWrapper = new HadoopInputFormat(inputFormat, key, value, jobConf);


		return inputFormatWrapper;
	}

	private HadoopMapFunction getMapFunction(JobConf jobConf) {
		HadoopMapFunction<?,?,?,?> mapFunctionWrapper = new HadoopMapFunction<WritableComparable, Writable, WritableComparable, Writable>(jobConf);
		return mapFunctionWrapper;
	}

	private HadoopReduceFunction getReduceFunction(JobConf jobConf) {
		HadoopReduceFunction<?,?,?,?> reduceFunctionWrapper = new HadoopReduceFunction<WritableComparable, Writable, WritableComparable, Writable>(jobConf);
		return reduceFunctionWrapper;

	}

	private HadoopOutputFormat getOutputFormat(JobConf jobConf) {
		HadoopOutputFormat<?,?> outputFormatWrapper = new HadoopOutputFormat<WritableComparable, Writable>(jobConf.getOutputFormat() ,hadoopJobConf);
		return outputFormatWrapper;
	}

	@Override
	public void setConf(Configuration configuration) {
		this.hadoopJobConf = (JobConf) configuration;
	}

	@Override
	public Configuration getConf() {
		return this.hadoopJobConf;
	}

	public static void main(String[] args) throws Exception{
		final String inputPath = args[0];
		final String outputPath = args[1];
		JobConf c = new JobConf();
		c.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
		org.apache.hadoop.mapred.TextInputFormat.addInputPath(c, new Path(inputPath));
		c.setOutputFormat(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(c, new Path(outputPath));
		c.setMapperClass(TestTokenizeMap.class);
		c.setOutputKeyClass(Text.class);
		c.setOutputValueClass(LongWritable.class);
		c.setReducerClass(LongSumReducer.class);
		c.set("mapred.textoutputformat.separator", " ");
		StratosphereJobClient.runJob(c);
	}


	public static class TestTokenizeMap<K> extends TokenCountMapper<K> {
		@Override
		public void map(K key, Text value, OutputCollector<Text, LongWritable> output,
						Reporter reporter) throws IOException{
			final Text strippedValue = new Text(value.toString().toLowerCase().replaceAll("\\W+", " "));
			super.map(key, strippedValue, output, reporter);
		}
	}


}
