package com.li.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class JavaWordCount {
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("");
        JavaSparkContext jsc=new JavaSparkContext(conf);
        JavaRDD<String> lines=jsc.textFile(args[0]);
        //切分压平
        JavaRDD<String> words=lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        //将单词和1组合在一起
        JavaPairRDD<String,Integer> wordAndOne=words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word,1);
            }
        });
        //聚合
        JavaPairRDD<String,Integer> reduce=wordAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        //调换次序
        JavaPairRDD<Integer,String> swaped=reduce.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tp) throws Exception {
//                return new Tuple2<>(tp._2,tp._1);
                return  tp.swap();
            }
        });
        //排序
        JavaPairRDD<Integer,String> sorted=swaped.sortByKey(false);
        //调整次序
        JavaPairRDD<String,Integer> result=sorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tp) throws Exception {
                return tp.swap();
            }
        });
        result.saveAsTextFile(args[1]);
        jsc.stop();
    }
}
