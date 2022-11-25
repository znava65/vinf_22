import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;

public class Main {
    public static HashMap<String, ArrayList<Animal>> index;
    public static final SparkConf config = new SparkConf()
            .setMaster("local[*]")
            .setAppName("SoccerParser");

    public static final JavaSparkContext jsc = new JavaSparkContext(config);
    public static final SQLContext sqlc = new SQLContext(jsc);
    public static void main(String[] args) {
        index = new HashMap<>();
        Preprocessor preprocessor = new Preprocessor("data_small/small.xml-p1p41242");
        try {
            JavaRDD<Animal> animals = preprocessor.parsePages();

            animals.foreach(a -> {
                System.out.println(a.getTitle());
                String[] tokens = a.getTitle().split("\\s");
                for (String token : tokens) {
                    if (!index.containsKey(token)) index.put(token, new ArrayList<>());
                    index.get(token).add(a);
                }
            });

//            System.out.println(index.get("bald").get(0).getTitle());
//            System.out.println(index.get("bald").get(0).getTitle());

        }
        catch (Exception e) {
            System.out.println(e.toString());
        }

        jsc.close();
    }
}
