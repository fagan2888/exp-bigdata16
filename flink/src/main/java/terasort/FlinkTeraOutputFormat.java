/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package terasort;

import java.io.IOException;

import org.apache.flink.api.java.record.io.FileOutputFormat;
import org.apache.flink.types.Record;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.hadoop.io.Text;
/**
 * The class is responsible for converting a two field record back into a line which is afterward written back to disk.
 * Each line ends with a newline character.
 * 
 */
public abstract class FlinkTeraOutputFormat extends FileOutputFormat{
	private static final long serialVersionUID = 1L;

	/**
	 * A buffer to store the line which is about to be written back to disk.
	 */
	private final byte[] buffer = new byte[100];


	@Override
	public void writeRecord(Record record) throws IOException {
		this.stream.write(buffer, 0, buffer.length);
	}
    
	/* @Override */
	/* public void writeRecord(Tuple2<Text, Text> record) throws IOException { */
    /*     byte[] keyByteArray = record.getField(0).toString().getBytes(); */
    /*     for(int i = 0; i < 10; ++i) this.buffer[i] = keyByteArray[i]; */
    /*  */
    /*     byte[] valueByteArray = record.getField(1).toString().getBytes(); */
    /*     for(int i = 0; i < 90; ++i) this.buffer[i + 10] = keyByteArray[i]; */
	/* 	this.stream.write(buffer, 0, buffer.length); */
	/* } */

}
