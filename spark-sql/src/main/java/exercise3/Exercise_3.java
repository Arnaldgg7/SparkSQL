package exercise3;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.stream.Collectors;

public class Exercise_3 {

	public static void JSONqueries(SparkSession spark) {

		Dataset<Row> countries = spark.read().json("src/main/resources/nobel_prizes/country.json").toDF();
		Dataset<Row> prizes = spark.read().json("src/main/resources/nobel_prizes/prize.json").toDF();

		countries.createOrReplaceTempView("countries");
		prizes.createOrReplaceTempView("prizes");

		// Printing from a JSON file the 'code' and 'name' for each country that is listed in the 'countries' file:
		Dataset<Row> q1 = spark.sql("SELECT col.code, collect_list(col.name) FROM countries LATERAL VIEW explode(countries) v1 GROUP BY col.code ORDER BY col.code");
		JavaRDD<String> result = q1.toJavaRDD().map(t -> t.get(0) + " -- " + t.getList(1) + "\n");
		String output = result.collect().stream().collect(Collectors.joining());
		System.out.println(output);


		// For each year of the JSON file 'prize', printing the number of laureates that got a Nobel prize:
		Dataset<Row> q2 = spark.sql("SELECT t1.year, COUNT(t2.*) FROM prizes LATERAL VIEW explode(prizes) t1 as t1 LATERAL VIEW explode(t1.laureates) t2 as t2 GROUP BY t1.year ORDER BY t1.year");
		JavaRDD<String> result2 = q2.toJavaRDD().map(t -> t.get(0) + " -- " + t.get(1) + "\n");
		String output2 = result2.collect().stream().collect(Collectors.joining());
		System.out.println(output2);
	}

}


