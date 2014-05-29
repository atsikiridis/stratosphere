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
import eu.stratosphere.api.java.typeutils.ResultTypeQueryable;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.WritableTypeInfo;
import eu.stratosphere.hadoopcompatibility.mapred.utils.HadoopConfiguration;
import eu.stratosphere.hadoopcompatibility.mapred.wrapper.HadoopDummyReporter;
import eu.stratosphere.hadoopcompatibility.mapred.wrapper.HadoopOutputCollector;
import eu.stratosphere.types.TypeInformation;
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
public class HadoopMapFunction<KEYIN extends WritableComparable, VALUEIN extends Writable,
		KEYOUT extends WritableComparable, VALUEOUT extends Writable> extends FlatMapFunction<Tuple2<KEYIN,VALUEIN>,
		Tuple2<KEYOUT,VALUEOUT>> implements Serializable, ResultTypeQueryable<Tuple2<KEYOUT,VALUEOUT>> {

	private static final long serialVersionUID = 1L;

	private final transient Class<KEYOUT> keyoutClass;
	private final transient Class<VALUEOUT> valueoutClass;

	private JobConf jobConf;
	private Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> mapper;
	private HadoopOutputCollector<KEYOUT,VALUEOUT> outputCollector;
	private Reporter reporter;

	public HadoopMapFunction(JobConf jobConf) {
		this(jobConf, new HadoopOutputCollector<KEYOUT,VALUEOUT>(), new HadoopDummyReporter());
	}

	@SuppressWarnings("unchecked")
	public HadoopMapFunction(JobConf jobConf,
							HadoopOutputCollector<KEYOUT,VALUEOUT> outputCollector,
							Reporter reporter) {
		this.jobConf = jobConf;
		this.mapper = InstantiationUtil.instantiate(jobConf.getMapperClass());
		this.outputCollector = outputCollector;
		this.reporter = reporter;
		this.keyoutClass = (Class<KEYOUT>) this.jobConf.getMapOutputKeyClass();
		this.valueoutClass = (Class<VALUEOUT>) this.jobConf.getMapOutputValueClass();
	}

	@Override
	public void flatMap(Tuple2<KEYIN,VALUEIN> value, Collector<Tuple2<KEYOUT,VALUEOUT>> out) throws Exception {
		outputCollector.set(out);
		mapper.map(value.f0, value.f1, outputCollector, reporter);
	}

	@Override
	public TypeInformation<Tuple2<KEYOUT,VALUEOUT>> getProducedType() {
		final WritableTypeInfo<KEYOUT> keyTypeInfo = new WritableTypeInfo<KEYOUT>(keyoutClass);
		final WritableTypeInfo<VALUEOUT> valueTypleInfo = new WritableTypeInfo<VALUEOUT>(valueoutClass);
		return new TupleTypeInfo<Tuple2<KEYOUT,VALUEOUT>>(keyTypeInfo, valueTypleInfo);
	}

	/**
	 * Custom serialization methods.
	 *  @see http://docs.oracle.com/javase/7/docs/api/java/io/Serializable.html
	 */
	private void writeObject(ObjectOutputStream out) throws IOException {
		HadoopConfiguration.setOutputCollectorToConf(outputCollector.getClass(), jobConf);
		HadoopConfiguration.setReporterToConf(reporter.getClass(), jobConf);
		jobConf.write(out);
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException {
		jobConf = new JobConf();
		jobConf.readFields(in);
		try {
			this.mapper = InstantiationUtil.instantiate(this.jobConf.getMapperClass());
		} catch (Exception e) {
			throw new RuntimeException("Unable to instantiate the hadoop mapper", e);
		}
		ReflectionUtils.setConf(mapper, jobConf);
		outputCollector = InstantiationUtil.instantiate(HadoopConfiguration.getOutputCollectorFromConf(jobConf));
		reporter = InstantiationUtil.instantiate(HadoopConfiguration.getReporterFromConf(jobConf));
	}
}
