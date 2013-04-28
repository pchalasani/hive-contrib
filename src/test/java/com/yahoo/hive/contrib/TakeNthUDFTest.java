package com.yahoo.hive.contrib;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TakeNthUDFTest {

	@Test
	public void test() throws Exception{
		TakeNthUDF udf = new TakeNthUDF();
		udf.initialize(new ObjectInspector[] {
				ObjectInspectorFactory
						.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector),
				PrimitiveObjectInspectorFactory.javaIntObjectInspector });

		ArrayList<String> input = new ArrayList<String>();
		input.add("1");
		input.add("4");
		input.add("6");
		input.add("7");
		input.add("8");
		List<Text> results = udf.evaluate(new DeferredJavaObject[] {
				new DeferredJavaObject(input),
				new DeferredJavaObject(2) });
		assertEquals(results.size(), 3);
	}

}
