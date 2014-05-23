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

package eu.stratosphere.hadoopcompatibility.mapred.wrapper;

import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.util.Collector;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;

/**
 * A Hadoop OutputCollector that basically wraps a Stratosphere OutputCollector.
 * This implies that on each call of collect() the data is actually collected by Stratosphere.
 */
public class HadoopOutputCollector<OUT extends Tuple2>
		implements OutputCollector {

	private Collector<OUT> collector;

	public void set(Collector<OUT> collector) {
		this.collector = collector;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void collect(Object o, Object o2) throws IOException {
		final OUT tuple = (OUT) new Tuple2(o,o2);
		if (this.collector == null) {
			this.collector.collect(tuple);
		}
		else {
			throw new RuntimeException("There is no Stratosphere Collector set to be wrapped by this" +
					" HadoopOutputCollector object. The set method must be called in advance.");
		}
	}
}