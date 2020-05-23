package Parser;

import entity.Outlet;

public class OutletParser {

    public static Outlet parseCSV(String csvLine) {

        Outlet outlet = null;
        String[] csvValues = csvLine.split(",");

        outlet = new Outlet(
                csvValues[0], // data
                csvValues[1], // guariti
                csvValues[2] // tamponi
        );

        return outlet;
    }
}
