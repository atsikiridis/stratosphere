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

package eu.stratosphere.hadoopcompatibility.mapred.wrapper;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.Reporter;

/**
 * This is a progress monitor / reporter
 *
 */
public class HadoopReporter extends HadoopProgressable implements Reporter {

	private String status;
	private InputSplit split;

	@Override
	public void setStatus(String status) {
		this.status = status;
	}

	@Override
	public Counter getCounter(Enum<?> name) {
		return null ;//getCounter()
	}

	@Override
	public Counter getCounter(String group, String name) {
		return  null ;//new Counter()
	}

	@Override
	public void incrCounter(Enum<?> key, long amount) {

	}

	@Override
	public void incrCounter(String group, String counter, long amount) {

	}

	public void setInputSplit(InputSplit split) {
		this.split = split;
	}

	@Override
	public InputSplit getInputSplit() throws UnsupportedOperationException {
		if (split == null) {
			throw new UnsupportedOperationException("Input only available on map");
		} else {
			return split;
		}
	}

	@Override
	public float getProgress() {
		return 0.0f;
	}
}
