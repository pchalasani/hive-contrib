package com.yahoo.hive.contrib;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class ReseqUDFTest {

	@Test
	public void test() throws Exception {
		ReseqUDF udf = new ReseqUDF();
		udf.initialize(new ObjectInspector[]{
			PrimitiveObjectInspectorFactory.javaStringObjectInspector,
			PrimitiveObjectInspectorFactory.javaStringObjectInspector
		});
		List<Text> results = udf.evaluate(new DeferredJavaObject[]{
			new DeferredJavaObject("catid123value456score1catid321value654score2"),
			new DeferredJavaObject("\\d+")
		});
		assertEquals(results.size() , 6);
	}

}
