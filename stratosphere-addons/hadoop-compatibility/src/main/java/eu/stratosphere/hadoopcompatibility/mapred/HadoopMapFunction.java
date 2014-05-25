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

import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.hadoopcompatibility.mapred.utils.HadoopConfiguration;
import eu.stratosphere.hadoopcompatibility.mapred.wrapper.HadoopDummyReporter;
import eu.stratosphere.hadoopcompatibility.mapred.wrapper.HadoopOutputCollector;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.InstantiationUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * The wrapper for a Hadoop Mapper (mapred API).
 */

/*TODO Modify class signature as soon as the TypeExtractor supports non identical generic input/output types and type
erasure issues are fixed.*/
public class HadoopMapFunction<IN extends Tuple2<? extends WritableComparable,? extends Writable>, OUT extends Tuple2<? extends WritableComparable, ? extends Writable>>
		extends FlatMapFunction<IN, OUT> implements Serializable {

	private static final long serialVersionUID = 1L;

	private JobConf jobConf;
	private Mapper mapper;
	private String mapperName;
	private HadoopOutputCollector<OUT> outputCollector;
	private Reporter reporter;

	@SuppressWarnings("unchecked")
	public HadoopMapFunction(JobConf jobConf) {
		this(jobConf, new HadoopOutputCollector<OUT>(), new HadoopDummyReporter());
	}

	@SuppressWarnings("unchecked")
	public HadoopMapFunction(JobConf jobConf,
							HadoopOutputCollector<OUT> outputCollector,
							Reporter reporter) {
		this.jobConf = jobConf;
		this.mapper = InstantiationUtil.instantiate(jobConf.getMapperClass());
		this.mapperName = mapper.getClass().getName();
		this.outputCollector = outputCollector;
		this.reporter = reporter;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void flatMap(IN value, Collector<OUT> out) throws Exception {
		outputCollector.set(out);
		mapper.map(value.f0, value.f1, outputCollector, reporter);
	}

	/**
	 * Custom serialization methods.
	 *  @see http://docs.oracle.com/javase/7/docs/api/java/io/Serializable.html
	 */
	private void writeObject(ObjectOutputStream out) throws IOException {
		out.writeUTF(mapperName);
		HadoopConfiguration.setOutputCollectorToConf(outputCollector.getClass(), jobConf);
		HadoopConfiguration.setReporterToConf(reporter.getClass(), jobConf);
		jobConf.write(out);
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		mapperName = in.readUTF();
		if(jobConf == null) {
			jobConf = new JobConf();
		}
		jobConf.readFields(in);
		try {
			this.mapper = (Mapper) Class.forName(this.mapperName).newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Unable to instantiate the hadoop mapper", e);
		}
		ReflectionUtils.setConf(mapper, jobConf);
		outputCollector = InstantiationUtil.instantiate(HadoopConfiguration.getOutputCollectorFromConf(jobConf));
		reporter = InstantiationUtil.instantiate(HadoopConfiguration.getReporterFromConf(jobConf));
	}
}
