import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.Arrays;

public class Prova {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query3a");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.parallelize(Arrays.asList(
                "247.01601779755285,171.65250278086762,79.0329254727475,50.38442714126807,50.289655172413795",
                "114.38787878787879,13.93939393939394,8.157575757575758,8.121212121212121,7.321212121212121",
                "776.2435483870968,319.7165322580645,226.58629032258065,202.60927419354837,173.54798387096776",
                "17.377339901477832,6.395073891625616,3.5852216748768475,0.7344827586206897,0.5438423645320197"
        ));


        JavaRDD<Vector>pro=input.map(s -> {
            String[] sarray = s.split(",");
            double[] values = new double[sarray.length];
            for (int i = 0; i < sarray.length; i++) {
                values[i] = Double.parseDouble(sarray[i]);
            }
            return Vectors.dense(values);});
        for (Vector v:pro.collect()){
            System.out.println(v);
        }
        int numClusters = 3;
        int numIterations = 20;
        KMeansModel clusters = KMeans.train(pro.rdd(), numClusters, numIterations);
        System.out.println("\n*****Training*****");
        int clusterNumber = 0;
        for (Vector center: clusters.clusterCenters()) {
            System.out.println("Cluster center for Clsuter "+ (clusterNumber++) + " : " + center);
        }
        double cost = clusters.computeCost(pro.rdd());
        System.out.println("\nCost: " + cost);

    }
}
