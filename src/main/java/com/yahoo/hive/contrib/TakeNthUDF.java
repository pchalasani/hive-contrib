package com.yahoo.hive.contrib;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * picks every nth element from the array.
 * @author harshars
 *
 */
@Description(name = "nth",
value = "_FUNC_(x, n) - picks every nth element from an array",
extended = "Example:\n"
+ "  > SELECT _FUNC_('[1,2,5,3,4]','2') FROM src LIMIT 1;\n"
+ "  [2,3]\n")
public class TakeNthUDF extends GenericUDF {

	ObjectInspectorConverters.Converter inputConverter;
	ObjectInspectorConverters.Converter nthConverter;

	@Override
	public String getDisplayString(String[] children) {
		assert (children.length == 2);
		return "take-nth(" + children[0] + ", " + children[1] + ")";
	}

	@Override
	public List<Text> evaluate(DeferredObject[] arguments) throws HiveException {
		assert (arguments.length == 2);

		if (arguments[0].get() == null || arguments[1].get() == null) {
			return null;
		}
		@SuppressWarnings("unchecked")
		final ArrayList<Text> input = (ArrayList<Text>)inputConverter.convert(arguments[0].get());
		IntWritable n = (IntWritable)nthConverter.convert(arguments[1].get());
		final int v = n.get();
		final int size = (int)Math.round(Math.ceil(input.size() / (v + 0.0)));
		return new AbstractList<Text>(){

			@Override
			public Text get(int index) {
				// TODO Auto-generated method stub
				return input.get(v*index);
			}

			@Override
			public int size() {
				// TODO Auto-generated method stub
				return size;
			}
			
		};
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
		if (arguments.length != 2) {
			throw new UDFArgumentLengthException(
					"The function nth(s, regexp) takes exactly 2 arguments.");
		}
		inputConverter = ObjectInspectorConverters.getConverter(
				arguments[0],
				ObjectInspectorFactory
				.getStandardListObjectInspector(
						PrimitiveObjectInspectorFactory.writableStringObjectInspector));
		nthConverter = ObjectInspectorConverters.getConverter(
				arguments[1],
				PrimitiveObjectInspectorFactory.writableIntObjectInspector);
		
		return ObjectInspectorFactory
				.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
	}
}
