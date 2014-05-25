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
package eu.stratosphere.test.hadoopcompatibility.mapred;

import eu.stratosphere.hadoopcompatibility.mapred.example.Identity;
import eu.stratosphere.test.testdata.WordCountData;
import eu.stratosphere.test.util.JavaProgramTestBase;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;

public class HadoopLimitedJobITCase extends JavaProgramTestBase {
	
	protected String textPath;
	protected String resultPath;
	
	
	@Override
	protected void preSubmit() throws Exception {
		textPath = createTempFile("text.txt", WordCountData.TEXT);
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(WordCountData.TEXT, resultPath + "/1");
	}

	@Override
	/**
	 * Since this is a test where the key doesn't really make sense we omit it and keep the value to compare with.
	 */
	public void readAllResultLines(List<String> target, String resultPath, boolean inOrderOfFiles) throws IOException {
		for (BufferedReader reader : getResultReader(resultPath, inOrderOfFiles)) {
			String s;
			while ((s = reader.readLine()) != null) {
				String[] splits = s.split("\\s", 2);
				target.add(splits[1]);
			}
		}
	}
	
	@Override
	protected void testProgram() throws Exception {
		Identity.main(new String[]{textPath, resultPath});
	}
}