import java.util.Arrays;

import exercise1.Exercise_1;
import exercise2.Exercise_2;
import exercise3.Exercise_3;
import org.apache.spark.sql.SparkSession;

public class Main {
	
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));

		SparkSession spark;
		
		if (args.length < 1) {
			throw new Exception("Wrong number of parameters, usage: (exercise1,exercise2,exercise3); extra parameters specifics for the exercise");
		}

		if (args[0].equals("exercise1")) {
			if (args.length != 2) {
				throw new Exception("exercise1, extra parameters required: pathIn");
			}

			spark = SparkSession.builder().master("local[*]").appName("exercise1").getOrCreate();
			Exercise_1.SQLqueries(spark, args[1]);
			spark.stop();
		}
		else if (args[0].equals("exercise2")) {
			if (args.length != 2) {
				throw new Exception("exercise2, extra parameters required: pathIn");
			}
			spark = SparkSession.builder().master("local[*]").appName("exercise2").getOrCreate();
			Exercise_2.sparkSQLwithList(spark, args[1]);
			spark.stop();
		}
		else if (args[0].equals("exercise3")) {
			if (args.length != 1) {
				throw new Exception("exercise3");
			}
			spark = SparkSession.builder().master("local[*]").appName("exercise3").getOrCreate();
			Exercise_3.JSONqueries(spark);
			spark.stop();
		}
		else {
			throw new Exception("Wrong number of exercise");
		}
	}
}


