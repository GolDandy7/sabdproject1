package utility;

import org.apache.commons.math3.stat.regression.SimpleRegression;

import java.util.ArrayList;

public class TrendLine {
    public static double getSlope(ArrayList<Double> items,int size) {
        // creating regression object, passing true to have intercept term
        SimpleRegression simpleRegression = new SimpleRegression(true);
        // passing data to the model
        // model will be fitted automatically by the class
        for( int i=0;i<size;i++)
         simpleRegression.addData(i,items.get(i));
        // querying for model parameters
        return  simpleRegression.getSlope();
    }


}
