/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.hadoopcompatibility.mapred.example;

import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.hadoopcompatibility.mapred.HadoopMapFunction;
import eu.stratosphere.hadoopcompatibility.mapred.HadoopReduceFunction;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.hadoopcompatibility.mapred.HadoopInputFormat;
import eu.stratosphere.hadoopcompatibility.mapred.HadoopOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;

/**
 * Implements a Hadoop  job that simply passes through the mapper and the reducer
 * without modifying the input data and writes to disk the offset and the input line
 * after sorting them (Identity Function).
 * This example shows how a simple hadoop job can be run on Stratosphere.
 */
@SuppressWarnings("serial")
public class Identity {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: Identityt <input path> <result path>");
			return;
		}

		final String inputPath = args[0];
		final String outputPath = args[1];

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(1);

		JobConf hadoopJobConf = new JobConf();

		// Set up the Hadoop Input Format
		HadoopInputFormat<LongWritable, Text> hadoopInputFormat = new HadoopInputFormat<LongWritable, Text>(new TextInputFormat(),
				LongWritable.class, Text.class, hadoopJobConf);
		TextInputFormat.addInputPath(hadoopInputFormat.getJobConf(), new Path(inputPath));

		// Create a Stratosphere job with it
		DataSet<Tuple2<LongWritable, Text>> text = env.createInput(hadoopInputFormat);

		// Set the implementation of the hadoop mapper to be used in hadoop's configuration.
		hadoopJobConf.setMapperClass(IdentityMapper.class);
		// Use the information from hadoop's configuration to instantiate a stratosphere map function.
		FlatMapFunction<Tuple2<LongWritable, Text>, Tuple2<LongWritable, Text>> mapWrapper =
				(FlatMapFunction) new HadoopMapFunction<Tuple2<LongWritable,Text>>(hadoopJobConf);
		DataSet<Tuple2<LongWritable, Text>> words = text.flatMap(mapWrapper);


		// The same process for a reducer.
		hadoopJobConf.setReducerClass(IdentityReducer.class);
		hadoopJobConf.setCombinerClass(IdentityReducer.class);  // The same reducer implementation as a local combiner.
		GroupReduceFunction<Tuple2<LongWritable, Text>, Tuple2<LongWritable, Text>> reduceWrapper =
				new HadoopReduceFunction<Tuple2<LongWritable, Text>>(hadoopJobConf);
		DataSet<Tuple2<LongWritable, Text>> result = words.reduceGroup(reduceWrapper);



		// Set up Hadoop Output Format
		TextOutputFormat<LongWritable,Text> outputFormat =new TextOutputFormat<LongWritable, Text>();
		HadoopOutputFormat<LongWritable, Text> hadoopOutputFormat =
				new HadoopOutputFormat<LongWritable, Text>(outputFormat, hadoopJobConf);
		hadoopOutputFormat.getJobConf().set("mapred.textoutputformat.separator", " ");
		TextOutputFormat.setOutputPath(hadoopOutputFormat.getJobConf(), new Path(outputPath));

		// Output & Execute
		result.output(hadoopOutputFormat);
		env.execute("Identity");
	}
}
