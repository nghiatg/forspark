package pk;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
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
    static SparkConf conf = new SparkConf().setAppName("spark-sample").set("spark.sql.parquet.binaryAsString","true").setMaster("local[2]");
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
	String[] paths = new String[args.length - 2];
    	for(int i = 0 ; i < args.length - 2 ; ++i) {
    		paths[i] = args[i];
    	}
    	idk(paths,Integer.parseInt(args[args.length-2]),args[args.length - 1]);
	//joinTable(args[0],args[1],args[2],args[3]);
	//readParquet(args[0]).show(100);
    }

    public static void joinTable(String logPath, String title, String bannerPath, String output) throws Exception {
	    DataFrame log = readParquet(logPath);
            DataFrame titleCate = titleCate(title);
            DataFrame bannerCate = readParquet(bannerPath);
            //parquet.join(banner,parquet.col("bannerId").equalTo(banner.col("bannerid")).and(parquet.col("geo").gt(banner.col("banner_cat")))).show(
            DataFrame needToSave = log.join(titleCate,log.col("click_or_view").isNotNull().and(log.col("domain").equalTo(titleCate.col("domain"))).and(log.col("path").equalTo(titleCate.col("path"))))
                                            .join(bannerCate,log.col("bannerId").equalTo(bannerCate.col("bannerid"))).select(bannerCate.col("banner_cat"),titleCate.col("cate"),log.col("click_or_view"),log.col("domain"));
            needToSave.write().format("parquet").save(output);	
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
    
    public static void idk(String[] args, int limit,String output) throws Exception { 
	HashMap<Integer, Integer> count = new HashMap<>();
    	HashMap<Integer, String> match = new HashMap<>();
    	DataFrame df = sqlc.read().parquet(args);
    	df.registerTempTable("data");
    	DataFrame ratio = sqlc.sql("select click.banner_cat, click.cate,  click.count as click_amount, view.count as view_amount, click.count/view.count as ratio "
    			+ "from (select banner_cat, cate, click_or_view, count(*) as count from data "
    			+ "where click_or_view = true and cate <> -1 and cate is not null group by banner_cat,cate,click_or_view order by banner_cat,cate) as click, "
    			+ "(select banner_cat, cate, click_or_view, count(*) as count from data "
    			+ "where click_or_view = false and cate <> -1 and cate is not null group by banner_cat,cate,click_or_view order by banner_cat,cate) as view "
    			+ "where click.banner_cat = view.banner_cat and view.cate = click.cate order by ratio desc");
    	for(Row r : ratio.collect()) {
    		int cate = Integer.parseInt(r.getString(1).trim());
    		if(!count.containsKey(cate)) {
    			count.put(cate,0);
    		}
    		if(!match.containsKey(cate)) {
    			match.put(cate, "");
    		}
    		if(count.get(cate) > limit) {
    			continue;
    		}
    		count.put(cate,count.get(cate) + 1);
    		match.put(cate, match.get(cate) + " " + r.getInt(0) + " ");
    	}
    	ArrayList<Row> rows = new ArrayList<Row>();
    	for(Integer cate : count.keySet()) {
    		rows.add(RowFactory.create(cate,match.get(cate)));
    	}
    	StructType schema = new StructType(new StructField[] {
    		new StructField("cate",DataTypes.IntegerType,true,Metadata.empty()),
    		new StructField("banner_cat",DataTypes.StringType,true,Metadata.empty())	
    	});
    	DataFrame tke = sqlc.createDataFrame(rows, schema);
    	tke.write().parquet(output);
    }
}
