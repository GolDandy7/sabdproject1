package Parser;

public class OutletParser {

    public static Outlet parseCSV(String csvLine) {

        Outlet outlet = null;
        String[] csvValues = csvLine.split(",");



//            1464894,1377987280,3.216,0,1,0,3

        outlet = new Outlet(
                csvValues[0], // data
                csvValues[9], // guariti
                csvValues[12] // tamponi
        );

        return outlet;
    }
}
