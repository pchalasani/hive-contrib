package com.yahoo.hive.contrib;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

/**
 * Resequences a string, using the provided regex.
 * @author harshars
 *
 */
@Description(name = "reseq",
value = "_FUNC_(x, regex) - resequences the string based on regex",
extended = "Example:\n"
+ "  > SELECT _FUNC_('catid123value456score1catid234value344score2','\\d+') FROM src LIMIT 1;\n"
+ "  [123,456,1,234,344,2]\n")
public class ReseqUDF extends GenericUDF {
	private ObjectInspectorConverters.Converter[] converters;

	@Override
	public List<Text> evaluate(DeferredObject[] arguments) throws HiveException {
		assert (arguments.length == 2);

		if (arguments[0].get() == null || arguments[1].get() == null) {
			return null;
		}

		Text s = (Text) converters[0].convert(arguments[0].get());
		Text regex = (Text) converters[1].convert(arguments[1].get());
		Pattern pattern = Pattern.compile(regex.toString());
		Matcher matcher = pattern.matcher(s.toString());
		ArrayList<Text> result = new ArrayList<Text>();
		while (matcher.find()) {
			result.add(new Text(matcher.group()));
		}
		return result;
	}

	@Override
	public String getDisplayString(String[] children) {
		assert (children.length == 2);
		return "reseq(" + children[0] + ", " + children[1] + ")";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
		if (arguments.length != 2) {
			throw new UDFArgumentLengthException(
					"The function reseq(s, regexp) takes exactly 2 arguments.");
		}

		converters = new ObjectInspectorConverters.Converter[arguments.length];
		for (int i = 0; i < arguments.length; i++) {
			converters[i] = ObjectInspectorConverters
					.getConverter(
							arguments[i],
							PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		}

		return ObjectInspectorFactory
				.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
	}

	
}
