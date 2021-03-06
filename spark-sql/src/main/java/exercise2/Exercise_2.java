package exercise2;

import java.util.Arrays;
import java.util.List;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.row_number;

public class Exercise_2 {

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

	private static List<String> regions = Lists.newArrayList("Albania", "Andorra", "Armenia", "Austria", "Azerbaijan",
			"Belarus", "Belgium", "Bosnia and Herzegovina", "Bulgaria", "Croatia", "Cyprus", "Czech Republic",
			"Denmark", "Estonia", "Finland", "France", "Georgia", "Germany", "Greece", "Hungary", "Iceland", "Ireland",
			"Italy", "Kosovo", "Latvia", "Liechtenstein", "Lithuania", "Luxembourg", "Macedonia", "Malta", "Moldova",
			"Monaco", "Montenegro", "Netherlands", "Norway", "Poland", "Portugal", "Romania", "Russia", "San Marino",
			"Serbia", "Slovakia", "Slovenia", "Spain", "Sweden", "Switzerland", "Turkey", "Ukraine", "United Kingdom",
			"Vatican City (Holy See)"
	);

	public static void sparkSQLwithList(SparkSession spark, String pathIn) {
		List<StructField> fields = Lists.newArrayList();
		for (String field : schemaMap.keySet()) {
			fields.add(DataTypes.createStructField(field, schemaMap.get(field), false));
		}

		StructType schema = DataTypes.createStructType(fields);

		JavaRDD<String> inputWines = spark.read().textFile(pathIn).toJavaRDD();

		JavaRDD<Row> winesRDD = inputWines.map(t -> {
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


		StructType schema2 = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("name", DataTypes.StringType, false)));

		JavaRDD<String> inputRegions = spark.createDataset(regions, Encoders.STRING()).toJavaRDD();
		JavaRDD<Row> regionsRDD = inputRegions.map(RowFactory::create);

		Dataset<Row> regionsDataFrame = spark.createDataFrame(regionsRDD, schema2).withColumn("id", row_number().over(Window.orderBy("name")));
		regionsDataFrame.createOrReplaceTempView("regions");

		// Region name and average 'hue', grouped by region:
		// Here, we add '+1' to the modulo output, since we have to avoid output '0', as 'id' column starts in '1'.
		Dataset<Row> q1 = spark.sql("SELECT r.name, AVG(w.hue) as AVG_hue FROM regions as r JOIN wines as w ON w.region%50+1 = r.id GROUP BY r.name ORDER BY r.name");
		q1.show();
	}
}

