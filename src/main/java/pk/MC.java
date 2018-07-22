package pk;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class MC {
    static SparkConf conf = new SparkConf().setAppName("spark-sample").set("spark.sql.parquet.binaryAsString","true");
    static JavaSparkContext jsc = new JavaSparkContext(conf);
    static SQLContext sqlc = new SQLContext(jsc);
    static {
            try {
                    jsc.setLogLevel("ERROR");
            }catch(Exception e) {
                    e.printStackTrace();
            }
    }


    // args : 0 --- log path
    //                1 --- title category path
    //                2 --- banner category path
    //                3 --- output path
    public static void main(String[] args) throws Exception{

            DataFrame log = readParquet(args[0]);
            DataFrame titleCate = titleCate(args[1]);
            DataFrame bannerCate = bannreCate(args[2]);
            //parquet.join(banner,parquet.col("bannerId").equalTo(banner.col("bannerid")).and(parquet.col("geo").gt(banner.col("banner_cat")))).show(
            log.cache();
            titleCate.cache();
            bannerCate.cache();
            DataFrame needToSave = log.join(titleCate,log.col("domain").equalTo(titleCate.col("domain")).and(log.col("path").equalTo(titleCate.col("path"))))
                                            .join(bannerCate,log.col("bannerId").equalTo(bannerCate.col("bannerid"))).select("banner_cat","cate","click_or_view");
            needToSave.write().format("parquet").save(args[3]);

    }

    public static DataFrame readParquet(String logPath) throws Exception { 
            DataFrame df = sqlc.read().parquet(logPath);
            return df;
    }

    public static DataFrame bannreCate(String bannerCatePath) throws Exception { 
            BufferedReader br = new BufferedReader(new FileReader(bannerCatePath));
            String line = br.readLine();
            ArrayList<Row> rows = new ArrayList<Row>();
            while(line != null) {
                    rows.add(RowFactory.create(Integer.parseInt(line.split("\t")[0]),Integer.parseInt(line.split("\t")[1])));
                    line = br.readLine();
            }
            StructType schema = new StructType(new StructField[] {
                            new StructField("bannerid",DataTypes.IntegerType,false,Metadata.empty()),
                            new StructField("banner_cat",DataTypes.IntegerType,false,Metadata.empty())
            });
            br.close();
            return sqlc.createDataFrame(rows, schema);
    }

    public static DataFrame titleCate(String titleCatePath) throws Exception {
            DataFrame df = sqlc.read().parquet(titleCatePath);
            return df;
    }

}
