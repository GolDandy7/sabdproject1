package KmeansMlibSpark;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.mllib.linalg.Vectors;
import java.util.ArrayList;

public class KMeansSpark {
    /*private static Integer num_cluster=4;
    private static Integer num_iterazioni=20;

    public static ArrayList<Tuple2<Integer,String>>
    computeKMeans(Iterable<Tuple2<Double,String>> iter_tuple) {


        ArrayList<String> states = new ArrayList<>();
        ArrayList<Double> trends = new ArrayList<>();

        for (Tuple2<Double, String> k : iter_tuple) {
            states.add(k._2());
            trends.add(k._1());
        }
        double[] values = new double[trends.size()];
        for (int i = 0; i < trends.size(); i++) {
            values[i] = Double.parseDouble(String.valueOf(trends.get(i)));
        }
        //JavaRDD<Double> rdd = jsc.parallelize(trends);
       // JavaPairRDD<String,Double> pippo=rdd.mapToPair(x->new Tuple2<>("pluto",x));
        JavaRDD<Iterable<Double>> r = pippo.groupByKey().values();
        JavaRDD<Vector> parsedouble = r.map(new Function<Iterable<Double>, Vector>() {
            @Override
            public Vector call(Iterable<Double> doubles) throws Exception {
                double[] values=new double[trends.size()];
                int k=0;
                for(Double d:doubles){
                    values[k]=d.doubleValue();
                    k++;
                }
                return Vectors.dense(values);            }
        });

        for(Vector i:parsedouble.collect()){
            double []d=i.toArray();
            for(int k=0;k<i.size();k++){
                System.out.println(d[k]);
            }
        }
        KMeansModel clusters = KMeans.train(parsedouble.rdd(),num_cluster,num_iterazioni);
        ArrayList<Tuple2<Integer,String>> p= new ArrayList<>();
        jsc.stop();

        return p;

    }*/

}

