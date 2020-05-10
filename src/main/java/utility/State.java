package utility;

import java.io.Serializable;
import java.util.ArrayList;

public class State implements Serializable {

    private String country;
    private String lon;
    private String lat;
    private ArrayList<String> date;
    private ArrayList<String> sick_number;

    public State(String country, String lon, String lat, ArrayList<String> date, ArrayList<String> sick_number) {
        this.country = country;
        this.lon = lon;
        this.lat = lat;
        this.date = date;
        this.sick_number = sick_number;
    }

    public String getCountry() {
        return country;
    }

    public Float getLon() {
        return Float.parseFloat(lon);
    }

    public Float getLat() {
        return Float.parseFloat(lat);
    }

    public ArrayList<String> getDate() {
        return date;
    }

    public ArrayList<String> getSick_number() {
        return sick_number;
    }

    @Override
    public String toString() {
        return "State{" +
                "country='" + country + '\'' +
                ", lon='" + lon + '\'' +
                ", lat='" + lat + '\'' +
                ", date='" + date + '\'' +
                ", sick_number='" + sick_number + '\'' +
                '}';
    }
}
