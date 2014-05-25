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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;

/**
 * A Hadoop OutputCollector that basically wraps a Stratosphere OutputCollector.
 * This implies that on each call of collect() the data is actually collected by Stratosphere.
 */
public class HadoopOutputCollector<KEYOUT extends WritableComparable, VALUEOUT extends Writable>
		implements OutputCollector<KEYOUT,VALUEOUT> {

	private Collector<Tuple2<KEYOUT,VALUEOUT>> collector;

	public void set(Collector<Tuple2<KEYOUT,VALUEOUT>> collector) {
		this.collector = collector;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void collect(KEYOUT keyout, VALUEOUT valueout) throws IOException {
		final Tuple2<KEYOUT,VALUEOUT> tuple = new Tuple2(keyout,valueout);
		if (this.collector != null) {
			this.collector.collect(tuple);
		}
		else {
			throw new RuntimeException("There is no Stratosphere Collector set to be wrapped by this" +
					" HadoopOutputCollector object. The set method must be called in advance.");
		}
	}
}