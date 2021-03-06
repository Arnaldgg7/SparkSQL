package exercise1;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.List;


public class Exercise_1 {

	private static ImmutableMap<String, DataType> schemaMap = ImmutableMap.<String, DataType>builder()
			.put("type", DataTypes.StringType)
			.put("region", DataTypes.IntegerType)
			.put("alc", DataTypes.DoubleType)
			.put("m_acid", DataTypes.DoubleType)
			.put("ash", DataTypes.DoubleType)
			.put("alc_ash", DataTypes.DoubleType)
			.put("mgn", DataTypes.DoubleType)
			.put("t_phenols", DataTypes.DoubleType)
			.put("flav", DataTypes.DoubleType)
			.put("nonflav_phenols", DataTypes.DoubleType)
			.put("proant", DataTypes.DoubleType)
			.put("col", DataTypes.DoubleType)
			.put("hue", DataTypes.DoubleType)
			.put("od280od315", DataTypes.DoubleType)
			.put("proline", DataTypes.DoubleType)
			.build();

	public static void SQLqueries(SparkSession spark, String pathIn) {
		List<StructField> fields = Lists.newArrayList();
		for (String field : schemaMap.keySet()) {
			fields.add(DataTypes.createStructField(field, schemaMap.get(field), false));
		}

		StructType schema = DataTypes.createStructType(fields);

		JavaRDD<String> input = spark.read().textFile(pathIn).toJavaRDD();

		JavaRDD<Row> winesRDD = input.map(t -> {
			String[] col = t.split(",");
			return RowFactory.create(col[0],
					Integer.parseInt(col[1]),
					Double.parseDouble(col[2]),
					Double.parseDouble(col[3]),
					Double.parseDouble(col[4]),
					Double.parseDouble(col[5]),
					Double.parseDouble(col[6]),
					Double.parseDouble(col[7]),
					Double.parseDouble(col[8]),
					Double.parseDouble(col[9]),
					Double.parseDouble(col[10]),
					Double.parseDouble(col[11]),
					Double.parseDouble(col[12]),
					Double.parseDouble(col[13]),
					Double.parseDouble(col[14]));
			});

		Dataset<Row> winesDataFrame = spark.createDataFrame(winesRDD, schema);
		winesDataFrame.createOrReplaceTempView("wines");

		// Number of regions that have 'alc' greater than 11, and grouped by type:
		Dataset<Row> q1 = spark.sql("SELECT type, COUNT(DISTINCT region) as COUNT FROM wines WHERE alc > 11 GROUP BY type ORDER BY type");
		q1.show();

		// Descriptive statistics (count, mean, stddev, min and max) of the column 'proline':
		Dataset<Row> q2 = spark.sql("SELECT COUNT(proline) as COUNT, MEAN(proline) as MEAN, STD(proline) as STD, MIN(proline) as MIN, MAX(proline) as MAX FROM WINES");
		q2.show();
	}
	
}
