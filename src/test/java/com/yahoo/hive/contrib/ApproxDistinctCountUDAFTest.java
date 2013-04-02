package com.yahoo.hive.contrib;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import com.yahoo.eta.stats.cus.CountUniqueSketch;
import com.yahoo.eta.stats.cus.CountUniqueSketchSerialization;
import com.yahoo.hive.contrib.ApproxDistinctCountUDAF.ApproxDistinctCountAggBuffer;
import com.yahoo.hive.contrib.ApproxDistinctCountUDAF.SketchEvaluator;

public class ApproxDistinctCountUDAFTest {

	@Test
	public void testPositiveCompleteString() throws Exception {
		ApproxDistinctCountUDAF udaf = new ApproxDistinctCountUDAF();
		TypeInfo[] info = {TypeInfoFactory.stringTypeInfo};
		SketchEvaluator evaluator = udaf.getEvaluator(info);
		ObjectInspector[] parameters = {PrimitiveObjectInspectorFactory.javaStringObjectInspector};
		ObjectInspector oi = evaluator.init(Mode.COMPLETE, parameters);
		assertTrue(oi instanceof StructObjectInspector);
		ApproxDistinctCountAggBuffer aggBuffer = evaluator.getNewAggregationBuffer();
		String [] data = {"A","B","C","A","B","A"};
		for(String datum : data) {
			evaluator.iterate(aggBuffer, new Object[]{datum});
		}
		
		ArrayList<Object> results = evaluator.terminate(aggBuffer);
		DoubleWritable cardinality = (DoubleWritable) results.get(0);
		assertEquals(cardinality.get(), 3.0,0.1);
	}

	@Test
	public void testPositiveCompleteLong() throws Exception {
		ApproxDistinctCountUDAF udaf = new ApproxDistinctCountUDAF();
		TypeInfo[] info = {TypeInfoFactory.longTypeInfo};
		SketchEvaluator evaluator = udaf.getEvaluator(info);
		ObjectInspector[] parameters = {PrimitiveObjectInspectorFactory.javaLongObjectInspector};
		ObjectInspector oi = evaluator.init(Mode.COMPLETE, parameters);
		assertTrue(oi instanceof StructObjectInspector);
		ApproxDistinctCountAggBuffer aggBuffer = evaluator.getNewAggregationBuffer();
		long [] data = {120L,33L,120L,120L,44L};
		for(long datum : data) {
			evaluator.iterate(aggBuffer, new Object[]{datum});
		}
		
		ArrayList<Object> results = evaluator.terminate(aggBuffer);
		DoubleWritable cardinality = (DoubleWritable) results.get(0);
		assertEquals(cardinality.get(), 3.0,0.1);
	}
	@Test
	public void testPositiveCompleteBinary() throws Exception {
		ApproxDistinctCountUDAF udaf = new ApproxDistinctCountUDAF();
		TypeInfo[] info = {TypeInfoFactory.stringTypeInfo};
		SketchEvaluator evaluator = udaf.getEvaluator(info);
		ObjectInspector[] parameters = {PrimitiveObjectInspectorFactory.writableBinaryObjectInspector};
		ObjectInspector oi = evaluator.init(Mode.COMPLETE, parameters);
		assertTrue(oi instanceof StructObjectInspector);
		ApproxDistinctCountAggBuffer aggBuffer = evaluator.getNewAggregationBuffer();
		
		BytesWritable [] data = {
				newSerializedSketch("A"),
				newSerializedSketch("B"),
				newSerializedSketch("A"),
				newSerializedSketch("C")
		};
		for(BytesWritable datum : data) {
			evaluator.iterate(aggBuffer, new Object[]{datum});
		}
		
		ArrayList<Object> results = evaluator.terminate(aggBuffer);
		DoubleWritable cardinality = (DoubleWritable) results.get(0);
		assertEquals(cardinality.get(), 3.0,0.1);
	}
	
	private CountUniqueSketch newSketch(String datum) {
		CountUniqueSketch sketch = new CountUniqueSketch();
		sketch.update(datum);
		return sketch;
	}
	
	private BytesWritable newSerializedSketch(String datum) {
		byte[] b =  CountUniqueSketchSerialization.serializeSketch(newSketch(datum));
		return new BytesWritable(b);
	}
}
