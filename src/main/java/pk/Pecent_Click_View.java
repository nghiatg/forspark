package pk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class Pecent_Click_View {
	public static void tke(String fortkePath) throws IOException {
		ArrayList<String> domains = new ArrayList<>();
		domains.add("kenh14.vn");
		domains.add("dantri.com.vn");
		domains.add("cafef.vn");
		domains.add("nld.com.vn");
		domains.add("soha.vn");
		
//		SparkConf conf = new SparkConf().setAppName("update_db")
//				.set("spark.sql.parquet.binaryAsString", "true");
//		JavaSparkContext jsc = new JavaSparkContext(conf);
//		jsc.setLogLevel("ERROR");
//		SQLContext sqlContext = new SQLContext(jsc);
		DataFrame df = MC.sqlc.read().load(fortkePath);
		
		for(String d : domains) {
			System.out.println("\n\n" + d);
			DataFrame dfSite = df.where(df.col("domain").equalTo(d));
			JavaPairRDD<String, Long> counts = dfSite.toJavaRDD().mapToPair(new PairFunction<Row, String, Long>() {
				@Override
				public Tuple2<String, Long> call(Row row) throws Exception {
					// TODO Auto-generated method stub
					int banner_id = row.getInt(0);
					int[] index = Arrays.stream(row.getString(1).split(" ")).mapToInt(Integer::parseInt).toArray();
					boolean click_or_view = row.getBoolean(2);
					boolean check_similary = IntStream.of(index).anyMatch(x -> x == banner_id);
					if(click_or_view == true && check_similary == true) {
						String key = "similary_click";
						long value = 1;
						Tuple2<String,Long > rs1 = new Tuple2<String, Long>(key, value);
						return rs1;
					}
					if(click_or_view == true && check_similary == false) {
						String key = "dont_similary_click";
						long value = 1;
						Tuple2<String,Long > rs1 = new Tuple2<String, Long>(key, value);
						return rs1;
					}
					if(click_or_view == false && check_similary == true) {
						String key ="view_similary";
						long value = 1;
						Tuple2<String,Long > rs1 = new Tuple2<String, Long>(key, value);
						return rs1;
					}
					if(click_or_view == false && check_similary == false) {
						String key ="view_do't_similary";
						long value = 1;
						Tuple2<String,Long > rs1 = new Tuple2<String, Long>(key, value);	
						return rs1;
					}
					return null;
				}
			}).reduceByKey(new  Function2<Long, Long, Long>() {
				@Override
				public Long call(Long v1, Long v2) throws Exception {
					// TODO Auto-generated method stub
					return (v1+v2);
				}
			});
			StructType schema = new StructType(new StructField[] {
				new StructField("noname", DataTypes.StringType,true , Metadata.empty()),
				new StructField("thongke", DataTypes.LongType,true , Metadata.empty()),
			});
			JavaRDD<Row> tk = counts.map(new Function<Tuple2<String,Long>, Row>() {
				@Override
				public Row call(Tuple2<String, Long> v1) throws Exception {
					// TODO Auto-generated method stub
					String key = v1._1;
					long value = v1._2;	
					return RowFactory.create(key,value);
				}
			});
			DataFrame kq = MC.sqlc.createDataFrame(tk, schema);
			kq.show();
//			kq.write().format("parquet").save("/user/demtv/kq_tk");
		}
		
	}
}
