package Parser;

import entity.Outlet;

public class OutletParser {

    public static Outlet parseCSV(String csvLine) {

        Outlet outlet = null;
        String[] csvValues = csvLine.split(",");

        //TODO sistemare i valori delle colonne dopo preprocessing con Nifi
        outlet = new Outlet(
                csvValues[0], // data
                csvValues[9], // guariti
                csvValues[12] // tamponi
        );

        return outlet;
    }
}
