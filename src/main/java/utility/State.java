package utility;

import java.io.Serializable;
import java.util.ArrayList;

public class State implements Serializable {

    private String state;
    private String country;
    private String lon;
    private String lat;
    private ArrayList<String> sick_number;

    public String getState() {
        return state;
    }

    public State(String state, String country, String lon, String lat, ArrayList<String> sick_number) {
        this.state = state;
        this.country=country;
        this.lon = lon;
        this.lat = lat;
        this.sick_number = sick_number;
    }

    public String getCountry() {
        return state;
    }

    public Float getLon() {
        return Float.parseFloat(lon);
    }

    public Float getLat() {
        return Float.parseFloat(lat);
    }



    public ArrayList<String> getSick_number() {
        return sick_number;
    }

    @Override
    public String toString() {
        return "State{" +
                "state'=" + state + '\'' +
                "country='" + country + '\'' +
                ", lon='" + lon + '\'' +
                ", lat='" + lat + '\'' +
                ", sick_number='" + sick_number + '\'' +
                '}';
    }
}
