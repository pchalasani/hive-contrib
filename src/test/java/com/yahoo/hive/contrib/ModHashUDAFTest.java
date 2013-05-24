package com.yahoo.hive.contrib;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import com.yahoo.hive.contrib.ModHashUDAF.ModHashAggBuffer;
import com.yahoo.hive.contrib.ModHashUDAF.ModHashEvaluator;
import com.yahoo.streamlib.ModHash;

public class ModHashUDAFTest {

	@Test
	public void test() throws Exception{
		ModHashUDAF udaf = new ModHashUDAF();
		TypeInfo[] info = {TypeInfoFactory.stringTypeInfo};
		ModHashEvaluator evaluator = udaf.getEvaluator(info);
		ObjectInspector[] parameters = {
				PrimitiveObjectInspectorFactory.javaIntObjectInspector,
				PrimitiveObjectInspectorFactory.javaIntObjectInspector
		};
		ObjectInspector oi = evaluator.init(Mode.COMPLETE, parameters);
		assertTrue(oi instanceof BinaryObjectInspector);
		ModHashAggBuffer aggBuffer = evaluator.getNewAggregationBuffer();
		
		for(int i = 0; i< 100; i++) {
			evaluator.iterate(aggBuffer, new Object[]{i, 8});
		}
		
		BytesWritable results = evaluator.terminate(aggBuffer);
		byte[] bytes = results.getBytes();
		byte[] serialized = new byte[results.getLength()];
		System.arraycopy(bytes, 0, serialized, 0, serialized.length);
		ModHash h = ModHash.fromBytes(serialized);
		System.out.println(h);
	}

}
