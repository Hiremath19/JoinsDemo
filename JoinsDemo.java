import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JoinsDemo {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Sankir");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);


        List<Tuple2<String,Integer>> list1 = new ArrayList<>();
        list1.add(new Tuple2("Ramesh", 20));
        list1.add(new Tuple2("Suresh", 30));
        list1.add(new Tuple2("Mahesh", 10));
        list1.add(new Tuple2("Dinesh", 40));
        list1.add(new Tuple2("Ramesh", 50));


        List<Tuple2<String, Integer>> list2 = Arrays.asList(new Tuple2("Ramesh", 50), new Tuple2("Mahesh", 30),
                new Tuple2("Dinesh", 60), new Tuple2("Prakash", 80));

        JavaPairRDD<String,Integer> RDD1 = sc.parallelizePairs(list1);
        JavaPairRDD<String,Integer> RDD2 = sc.parallelizePairs(list2);

        System.out.println(RDD1.collect());
        System.out.println(RDD2.collect());

        System.out.println("RDD1 subtract by key RDD2: ");
        System.out.println(RDD1.subtractByKey(RDD2).collect());

        System.out.println("RDD1 join RDD2:");
        System.out.println(RDD1.join(RDD2).collect());

        System.out.println("RDD1 LeftOuterJoin RDD2:");
        System.out.println(RDD1.leftOuterJoin(RDD2).collect());

        RDD1.leftOuterJoin(RDD2).foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, Optional<Integer>>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Integer, Optional<Integer>>> s) throws Exception {
                System.out.println(s._1 + " " + s._2._1 + " " + s._2._2.orElse(0));
            }
        });


        System.out.println("RDD1 RightOuterJoin RDD2:");
        System.out.println(RDD1.rightOuterJoin(RDD2).collect());

        RDD1.rightOuterJoin(RDD2).foreach(new VoidFunction<Tuple2<String, Tuple2<Optional<Integer>, Integer>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Optional<Integer>, Integer>> s) throws Exception {
                System.out.println(s._1 + ":" + s._2._1.or(0) + "," +  s._2._2);
            }
        });


        System.out.println("RDD1 cogroup RDD2:");
        System.out.println(RDD1.cogroup(RDD2).collect());

    }
}
