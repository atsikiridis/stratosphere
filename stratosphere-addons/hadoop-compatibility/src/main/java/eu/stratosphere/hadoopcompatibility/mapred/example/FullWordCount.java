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


import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.operators.Grouping;
import eu.stratosphere.hadoopcompatibility.mapred.HadoopMapFunction;
import eu.stratosphere.hadoopcompatibility.mapred.HadoopReduceFunction;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.hadoopcompatibility.mapred.HadoopInputFormat;
import eu.stratosphere.hadoopcompatibility.mapred.HadoopOutputFormat;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapred.lib.TokenCountMapper;

import java.io.IOException;
import java.io.Serializable;

/**
 * Implements a Hadoop  job that simply passes through the mapper and the reducer
 * without modifying the input data and writes to disk the offset and the input line
 * after sorting them (Identity Function).
 * This example shows how a simple hadoop job can be run on Stratosphere.
 */
@SuppressWarnings("serial")
public class FullWordCount {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: FulllWordCount <input path> <result path>");
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

		//Set the mapper implementation to be used.
		hadoopJobConf.setMapperClass(TestTokenizeMap.class);
		DataSet<Tuple2<Text, LongWritable>> words = text.flatMap( new HadoopMapFunction<LongWritable, Text,
				Text, LongWritable>(hadoopJobConf){});

		hadoopJobConf.setReducerClass(LongSumReducer.class);
		hadoopJobConf.setCombinerClass(LongSumReducer.class);  // The same reducer implementation as a local combiner.

		Grouping<Tuple2<Text, LongWritable>> grouping = words.groupBy(0); //FIXME The grouping has a bug, see #860
		DataSet<Tuple2<Text,LongWritable>> result = grouping.reduceGroup(new CombinableReduceFunction(hadoopJobConf));

        TextOutputFormat outputFormat = new TextOutputFormat<Text, LongWritable>();
		HadoopOutputFormat<Text, LongWritable> hadoopOutputFormat =
				new HadoopOutputFormat<Text, LongWritable>(outputFormat, hadoopJobConf);
		hadoopOutputFormat.getJobConf().set("mapred.textoutputformat.separator", " ");
		TextOutputFormat.setOutputPath(hadoopOutputFormat.getJobConf(), new Path(outputPath));

		// Output & Execute
		result.output(hadoopOutputFormat);
		env.execute("FullWordCount");
	}

	public static class TestTokenizeMap<K> extends TokenCountMapper<K> {
		@Override
		public void map(K key, Text value, OutputCollector<Text, LongWritable> output,
						Reporter reporter) throws IOException{
			Text strippedValue = new Text(value.toString().toLowerCase().replaceAll("\\W+", " "));
			super.map(key, strippedValue, output, reporter);
		}
	}

	/**
	 * Due to the fact that we have to subclass the HadoopReduceFunction and annotations are not inheritable, for now
	 * we have this class.
	 */
	@GroupReduceFunction.Combinable
	public static class CombinableReduceFunction extends HadoopReduceFunction<Text, LongWritable, Text, LongWritable>
			implements Serializable {

		public CombinableReduceFunction(JobConf jobConf) {
			super(jobConf);
		}

	}
}
