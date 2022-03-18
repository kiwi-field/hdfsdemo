package hive;//package com.mashibing;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 脱敏
 * 需要打jar包，将jar包上传到hive> add jar my_jar.jar;
 *
 * hdfs dfs -mkdir /jar
 * hdfs dfs -put hivedemo.jar /jar
 * CREATE FUNCTION tm AS 'hive.tuomin' USING JAR 'hdfs://mycluster/jar/hivedemo.jar'
 *
 */
public class TuoMin extends UDF {

	public Text evaluate(final Text s) {
		if (s == null) {
			return null;
		}
		String str = s.toString().substring(0, 1) + "bjmsb";
		return new Text(str);
	}

}
