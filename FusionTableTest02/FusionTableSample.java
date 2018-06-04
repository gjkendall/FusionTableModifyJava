/*
 * Program to modify rows in a Google Fusion Table.  By Greg Kendall.
 * If you need assistance with this you can contact Greg at: kendall (dot) greg (at) gmail (dot) com
 * I am using this code to learn how to modify fields in rows of a fusion table. I am also testing
 * calling a kotlin routine from java. This code was based on Google examples of how to access
 * a fusion table.  I am a novice at Java.
 *
 * This code also uses the geonames.org API to get names of states and cities near locations.
 *
 * Some code Copyright (c) 2012 Google Inc. (Basic access to Fusion Tables).
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

//package FusionTableTest02;


import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.DateTime;
import com.google.api.client.util.store.DataStoreFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.fusiontables.Fusiontables;
import com.google.api.services.fusiontables.Fusiontables.Query.Sql;
import com.google.api.services.fusiontables.Fusiontables.Table.Delete;
import com.google.api.services.fusiontables.FusiontablesScopes;
import com.google.api.services.fusiontables.model.Column;
import com.google.api.services.fusiontables.model.Sqlresponse;
import com.google.api.services.fusiontables.model.Table;
import com.google.api.services.fusiontables.model.TableList;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.Scanner;

import java.io.BufferedReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.geonames.Toponym;
import org.geonames.ToponymSearchCriteria;
import org.geonames.ToponymSearchResult;
import org.geonames.WebService;

import org.json.*;
//import org.json.simple.JSONArray;
//import org.json.simple.JSONObject;
import org.json.simple.parser.*;


/**
 * @author Christian Junk: Enhanced by Greg Kendall
 *
 */
class FusionTablesSample {

    /**
     * Be sure to specify the name of your application. If the application name is {@code null} or
     * blank, the application will log a warning. Suggested format is "MyCompany-ProductName/1.0".
     */
    private static final String APPLICATION_NAME = "FusionTest02";

    /**
     * Directory to store user credentials.  Note: Google code adds /out/production/classes to
     * find credentials. <user.home>/<project directory>/out/production/classes/client_secrets.json
     */
    private static final java.io.File DATA_STORE_DIR =
            new java.io.File(System.getProperty("user.home"), "IdeaProjects/FusionTableTest02");

    /**
     * Global instance of the {@link DataStoreFactory}. The best practice is to make it a single
     * globally shared instance across your application.
     */
    private static FileDataStoreFactory dataStoreFactory;

    /**
     * Global instance of the HTTP transport.
     */
    private static HttpTransport httpTransport;

    /**
     * Global instance of the JSON factory.
     */
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    private static Fusiontables fusiontables;

    private static String sqlupdate;
    private static List<List<Object>> mylist; // list in a list (Rows, Columns)
    private static String newAreaName;
    private static final int ROW_ID = 0;
    private static final int AREA_NAME = 1;
    private static final int NOTES = 2;
    private static final int NUMBER = 3;
    private static final int LOCATION = 4;
    private static final int STATE = 5;
    private static final int CODES = 6;

    private static int count;
    private static boolean mtest = false;

    /**
     * Authorizes the installed application to access user's protected data.
     */
    private static Credential authorize() throws Exception {
        // load client secrets
        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(
                JSON_FACTORY, new InputStreamReader(
                        FusionTablesSample.class.getResourceAsStream("/client_secrets.json")));
        if (clientSecrets.getDetails().getClientId().startsWith("Enter")
                || clientSecrets.getDetails().getClientSecret().startsWith("Enter ")) {
            System.out.println(
                    "Enter Client ID and Secret from https://code.google.com/apis/console/?api=fusiontables "
                            + "into fusiontables-cmdline-sample/src/main/resources/client_secrets.json");
            System.exit(1);
        }
        // set up authorization code flow
        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
                httpTransport, JSON_FACTORY, clientSecrets,
                Collections.singleton(FusiontablesScopes.FUSIONTABLES)).setDataStoreFactory(
                dataStoreFactory).build();
        // authorize
        return new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
    }

    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.println("Test Y/N: ");
        String isTest = sc.next();
        if (isTest == "Y") mtest=true;
        sc.close();
        try {
            setupaccess();
            // run commands
//            listTables();
//            String tableId = createTable();
//            insertData(tableId);
            String tableID = "1Vq6h4j1oZ3XS5h-UhB8GtXMau4n5fqmMfBEkgYcY"; //main table
//            String tableID = "1c-P1t1fJ7NCIqoPHzNT5pMjUFHa7ZDtP8RU78BcW";  //copy table
            getRows(tableID);

            updateRows(tableID);
//            getState();
//            deleteTable(tableId);
            return;
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (Throwable t) {
            t.printStackTrace();
        }
        System.exit(1);
    }

    /**
     * @param tableId
     * @throws IOException
     */
    private static void getRows(String tableId) throws IOException {
        View.header("Updating Rows From Table");
        /*Sql sql = fusiontables.query().sql("SELECT RowID, 'Area Name', Notes, Number FROM " + tableId +
                " Where Manager = '' AND 'Review 1' CONTAINS IGNORING CASE '.fs.' Order by Number ASC LIMIT 3000");*/
        /*Sql sql = fusiontables.query().sql("SELECT RowID, 'Area Name', Notes, Number FROM " + tableId +
                " Where 'Area Name' CONTAINS IGNORING CASE 'Tioga George' Order by Number ASC LIMIT 3000");*/
        /*Sql sql = fusiontables.query().sql("SELECT RowID, 'Area Name', Notes, Number FROM " + tableId +
                " Where 'Area Name' ='' Order by Number DESC LIMIT 2000");*/
        /*Sql sql = fusiontables.query().sql("SELECT RowID, 'Area Name', Notes, Number FROM " + tableId +
                " Where 'Area Name' CONTAINS 'X01' Order by Number DESC LIMIT 1"); */
        /*AND 'City (nearest)' DOES NOT CONTAIN IGNORING CASE 'Mexico'*/

        /*Sql sql = fusiontables.query().sql("SELECT RowID, 'Area Name', Notes, Number, Location FROM " + tableId +
                " Where State = '' Order by Number DESC LIMIT 100");*/
        /*Sql sql = fusiontables.query().sql("SELECT RowID, 'Area Name', Notes, Number, Location FROM " + tableId +
                " Where State = 'BCS' Order by Number DESC LIMIT 100");*/
        Sql sql = fusiontables.query().sql("SELECT RowID, 'Area Name', Notes, Number, Location, State, Codes FROM " + tableId +
                " Where State = 'ID' AND 'City (nearest)' = '' Order by Number DESC LIMIT 100");

        try {
            Sqlresponse response = sql.execute();
            // System.out.println(response.toPrettyString());

            mylist = response.getRows();

        } catch (IllegalArgumentException e) {
            // For google-api-services-fusiontables-v1-rev1-1.7.2-beta this exception will always
            // been thrown.
            // Please see issue 545: JSON response could not be deserialized to Sqlresponse.class
            // http://code.google.com/p/google-api-java-client/issues/detail?id=545
        }
    }
    
    private static void updateRows(String tableId) throws IOException {
        // IOException needed  ParseException
        count = 1;
        mylist.forEach((myRow) -> {
            try {
                // modify fields in table...
                //newAreaName = kt.firstpart(myRow.get(NOTES).toString()); //get Notes first sentence
                //newAreaName = newAreaName.replace("'", "''");
                //newAreaName += " X01";
                //String state = getStateFrmLoc(myRow.get(LOCATION).toString());
                //String state = "MX-BCS";
                float km;
                if ( "AK,MT,NV".contains(myRow.get(STATE).toString()) ) {
                    km = 180f; // 111.85 miles
                } else {
                    km = 80.5f;  // 50 miles
                }

                BigCity big = new BigCity(myRow.get(LOCATION).toString(), km);
                String cityState = big.cityName +", "+big.state;

                if (big.population < 10000f) {
                    System.out.println("Skip for low population :"+myRow.get(NUMBER));
                } else {

                    sqlupdate = "UPDATE " + tableId + " " +
                            "SET 'City (nearest)' = '" + cityState + "' " +
                            ",'Codes' = '" + myRow.get(CODES).toString() + ",#U1' " +
                            "WHERE ROWID = " + myRow.get(ROW_ID);
                    System.out.println("[" + count + "]" + myRow.get(NUMBER) + ": " + sqlupdate);

                    // do the update...
                    if (!mtest) {  // if testing then don't update
                        sql_doupdate(sqlupdate);
                    }
                    count++;
                    if ((count % 30) == 0) {
                        System.out.println("waiting 60 seconds");
                        TimeUnit.SECONDS.sleep(60); //Fusion Tables allows 30 updates then must wait 1 minute.
                    }
                }
                } catch(Exception e){
                    System.out.println(e.getMessage());
            }

        });

    }

    private static void sql_doupdate(String sqlupdate) throws IOException {
        Sql sql2 = fusiontables.query().sql(sqlupdate);
        try {
            Sqlresponse response2 = sql2.execute();
        } catch (IllegalArgumentException e) {
            // For google-api-services-fusiontables-v1-rev1-1.7.2-beta this exception will always
            // been thrown.
            // Please see issue 545: JSON response could not be deserialized to Sqlresponse.class
            // http://code.google.com/p/google-api-java-client/issues/detail?id=545
        }
    }

    /**
     * List tables for the authenticated user.
     */
    private static void listTables() throws IOException {
        View.header("Listing My Tables");

        // Fetch the table list
        Fusiontables.Table.List listTables = fusiontables.table().list();
        TableList tablelist = listTables.execute();

        if (tablelist.getItems() == null || tablelist.getItems().isEmpty()) {
            System.out.println("No tables found!");
            return;
        }

        for (Table table : tablelist.getItems()) {
            View.show(table);
            View.separator();
        }
    }

    /**
     * Create a table for the authenticated user.
     */
    private static String createTable() throws IOException {
        View.header("Create Sample Table");

        // Create a new table
        Table table = new Table();
        table.setName(UUID.randomUUID().toString());
        table.setIsExportable(false);
        table.setDescription("Sample Table");

        // Set columns for new table
        table.setColumns(Arrays.asList(new Column().setName("Text").setType("STRING"),
                new Column().setName("Number").setType("NUMBER"),
                new Column().setName("Location").setType("LOCATION"),
                new Column().setName("Date").setType("DATETIME")));

        // Adds a new column to the table.
        Fusiontables.Table.Insert t = fusiontables.table().insert(table);
        Table r = t.execute();

        View.show(r);

        return r.getTableId();
    }

    /**
     * Inserts a row in the newly created table for the authenticated user.
     */
    private static void insertData(String tableId) throws IOException {
        Sql sql = fusiontables.query().sql("INSERT INTO " + tableId + " (Text,Number,Location,Date) "
                + "VALUES (" + "'Google Inc', " + "1, " + "'1600 Amphitheatre Parkway Mountain View, "
                + "CA 94043, USA','" + new DateTime(new Date()) + "')");

        try {
            sql.execute();
        } catch (IllegalArgumentException e) {
            // For google-api-services-fusiontables-v1-rev1-1.7.2-beta this exception will always
            // been thrown.
            // Please see issue 545: JSON response could not be deserialized to Sqlresponse.class
            // http://code.google.com/p/google-api-java-client/issues/detail?id=545
        }
    }

    /**
     * Deletes a table for the authenticated user.
     */
    private static void deleteTable(String tableId) throws IOException {
        View.header("Delete Sample Table");
        // Deletes a table
        Delete delete = fusiontables.table().delete(tableId);
        delete.execute();
    }


    public static void setupaccess() {
        try {
            httpTransport = GoogleNetHttpTransport.newTrustedTransport();
            dataStoreFactory = new FileDataStoreFactory(DATA_STORE_DIR);
            // authorization
            Credential credential = authorize();
            // set up global FusionTables instance
            fusiontables = new Fusiontables.Builder(
                    httpTransport, JSON_FACTORY, credential).setApplicationName(APPLICATION_NAME).build();
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public static void getState() {  //doing example from geonames.org
        try {
            WebService.setUserName("gjkendall"); // add your username here

            ToponymSearchCriteria searchCriteria = new ToponymSearchCriteria();
            searchCriteria.setQ("zurich");
            ToponymSearchResult searchResult = WebService.search(searchCriteria);
            for (Toponym toponym : searchResult.getToponyms()) {
                System.out.println(toponym.getName() + " " + toponym.getCountryName());
            }
        } catch (Exception e){
            //return null;
        }

    }

    public static String getURL(String p_url) {
        //given a RESTful URL return the JSON output. Currently only one line output.
        try {

            URL url = new URL(p_url);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");

            if (conn.getResponseCode() != 200) {
                throw new RuntimeException("Failed : HTTP error code : "
                        + conn.getResponseCode());
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(
                    (conn.getInputStream())));

            String output;
            //System.out.println("Output from Server .... \n");
            while ((output = br.readLine()) != null) {
                //System.out.println(output);
                return output;  // need to fix this to handle multi-line JSON output.
            }

            conn.disconnect();
            return output;

        } catch (MalformedURLException e) {

            e.printStackTrace();

        } catch (IOException e) {

            e.printStackTrace();

        }
        return "";
    }

    public static String getJSONitem(String jsonString, String item) throws ParseException
    {
        JSONObject obj = new JSONObject(jsonString);

        // getting item like adminCode1
        String out = obj.getString(item);

        //System.out.println("getJSONitem:" + out);
        return out;
    }



    public static String getStateFrmLoc( String location ) throws ParseException
            // {"codes":[{"code":"7","level":"1","type":"ISO3166-2"}],"adminCode1":"07","distance":0,"countryCode":"AT","countryName":"Austria","adminName1":"Tyrol"}
    {
        String[] parts = location.replaceAll("\\s","").split(","); //remove spaces and split on ,
        String url = "http://api.geonames.org/countrySubdivisionJSON?lat=" + parts[0] + "&lng=" +
                parts[1] + "&username=gjkendall";

        //System.out.println("JSON: " + url);


        String output = getURL(url);
        //System.out.println("getStateFrmLoc - JSON: "+output);
        String state = getJSONitem(output, "adminCode1");
        //System.out.println("getStateFrmLoc: "+state);
        return state;
    }

}

class BigCity // BigCityFromLoc
{
    public long population;
    public String cityName;
    public String state;

    public BigCity(String location, float km) throws ParseException
    {
        //json = "{\"lng\":-106.3069722,\"geonameId\":5476825,\"countrycode\":\"US\",\"name\":\"Los Alamos\",\"fclName\":\"city, village,...\",\"toponymName\":\"Los Alamos\",\"fcodeName\":\"seat of a second-order administrative division\",\"wikipedia\":\"en.wikipedia.org/wiki/Los_Alamos%2C_New_Mexico\",\"lat\":35.8880796,\"fcl\":\"P\",\"population\":12019,\"fcode\":\"PPLA2\"}";

        BoundingBox bx = new BoundingBox(location, km); // gps coord,km from location

        String url = "http://api.geonames.org/citiesJSON?" +
                "north=" + bx.north + "&south=" + bx.south + "&east=" + bx.east + "&west=" + bx.west +
                "&lang=de&username=gjkendall&maxRows=1";

        String jsonString = FusionTablesSample.getURL(url);
        //System.out.println("JSON: "+output);

        JSONObject obj = new JSONObject(jsonString);
        JSONArray arr = obj.getJSONArray("geonames");

        String city = arr.getJSONObject(0).getString("name");
        long pop = arr.getJSONObject(0).getLong("population");

        Double biglat = arr.getJSONObject(0).getDouble("lat");
        Double biglng = arr.getJSONObject(0).getDouble("lng");
        String bigloc = biglat.toString()+", "+biglng.toString();
        String st = FusionTablesSample.getStateFrmLoc(bigloc); // need the state too for this location.

        population = pop;
        cityName = city;
        state = st;

    }
}




// Compute bounding Box coordinates for use with Geonames API.
class BoundingBox
{
    public double north, south, east, west;
    public BoundingBox(String location, float km)
    {
        //System.out.println(location + " : "+ km);
        String[] parts = location.replaceAll("\\s","").split(","); //remove spaces and split on ,

        double lat = Double.parseDouble(parts[0]);
        double lng = Double.parseDouble(parts[1]);

        double adjust = .008983112; // 1km in degrees at equator.
        //adjust = 0.008983152770714983; // 1km in degrees at equator.

        //System.out.println("deg: "+(1.0/40075.017)*360.0);


        north = lat + ( km * adjust);
        south = lat - ( km * adjust);

        double lngRatio = 1/Math.cos(Math.toRadians(lat)); //ratio for lng size
        //System.out.println("lngRatio: "+lngRatio);

        east = lng + (km * adjust) * lngRatio;
        west = lng - (km * adjust) * lngRatio;
    }

}

class View {

    static void header(String name) {
        System.out.println();
        System.out.println("================== " + name + " ==================");
        System.out.println();
    }

    static void show(Table table) {
        System.out.println("id: " + table.getTableId());
        System.out.println("name: " + table.getName());
        System.out.println("description: " + table.getDescription());
        System.out.println("attribution: " + table.getAttribution());
        System.out.println("attribution link: " + table.getAttributionLink());
        System.out.println("kind: " + table.getKind());

    }

    static void separator() {
        System.out.println();
        System.out.println("------------------------------------------------------");
        System.out.println();
    }


}