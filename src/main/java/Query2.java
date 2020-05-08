import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.regex.Pattern;

public class Query2 {

    private static String pathToFile = "src/dataset/covid19datiinternazionali.csv";
    private static Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query2");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //sc.setLogLevel("ERROR");
        JavaRDD<String> raws = sc.textFile(pathToFile);

        /*
            1. tolgo la prima riga del file contenente il nome delle colonne
            2. splitto la riga per trovare il nome delle colonne
         */
        String firstRow = raws.first();
        String[] colnames = firstRow.split(",");
        JavaRDD<String> covid_data = raws.filter(x -> !x.equals(firstRow));

        System.out.println(covid_data.first());
        sc.stop();

    }
}
