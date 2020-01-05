import java.io.*;
import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;
import java.util.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.SQLException;

public class JSonConverter
{
  public static void main(String[] args)
  {
    String filePath = "C:\\Users\\babst\\Desktop\\Assignment\\src\\main\\java\\hello\\logfile.txt";
    JSonConverter js = new JSonConverter();
    //convert the entire text to string
    String jsonText =js.findFileInformation(filePath);
    List<JSONObject> jsonObjects = js.getJSonData(jsonText);
    //call the function that sorts the data by id
    JSONArray sortedJSonArray = js.sortDataByID(jsonObjects);
    js.createEventDetailsTable();
    js.extractJsonData(sortedJSonArray);
  }

 public void createEventDetailsTable(){
    String url = "http://hsqldb.org/";
        
        // SQL statement for creating a new table
        String sql = "CREATE TABLE event+details (\n"
                + "    id varchar(8),\n"
                + "    duration INT,\n"
                + "    type varchar(20),\n"
                + "    host varchar(20),\n"
                + "    alert varchar(10)\n"
                + ");";
        
        try{
            Class.forName("org.hsqldb.jdbcDriver");
            Connection conn = DriverManager.getConnection(url);
                Statement stmt = conn.createStatement(); 
            // create a new table
            stmt.execute(sql);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
  
 }
 public String findFileInformation(String filePath)
  {
    String json = "";
    try {
    BufferedReader reader = new BufferedReader(new FileReader(filePath)); 
    StringBuilder sb = new StringBuilder();
    String line = reader.readLine();
    while (line != null){ 
      sb.append(line);
        sb.append("\n");
        line = reader.readLine();
    }
    json = sb.toString();
     System.out.println(json);
    } catch(IOException ex){
     System.out.println("File not found true");
    } catch(Exception ex){
       System.out.println("file not found");
    } finally {
       System.out.println("Job done");
    }
      return json;
  }
  
  public List<JSONObject> getJSonData(String json){
   //convert the entire string to a json Object
     JSONObject object = new JSONObject(json);
     JSONArray jsonArray =  object.getJSONArray("Events");  
    List<JSONObject> jsonValues = new ArrayList<JSONObject>();
    for(int i=0; i<jsonArray.length(); i++){
        jsonValues.add(jsonArray.getJSONObject(i));
     }
      return jsonValues;
  }
  
  public JSONArray sortDataByID(List jsonObjects){
        JSONArray sortedJsonArray = new JSONArray();
        Collections.sort( jsonObjects, new Comparator<JSONObject>() {
        //sort the list by id
        private static final String KEY_ID = "id";
        private static final String KEY_TIME = "timestamp";
        @Override
        public int compare(JSONObject a, JSONObject b) {
            String valA = new String();
            String valB = new String();

            try {
                valA = (String) a.get(KEY_ID);
                valB = (String) b.get(KEY_ID);
            } 
            catch (JSONException e) {
                System.out.println("The key you requested for is not present");
            }

            return valA.compareTo(valB);
            //if you want to change the sort order, simply use the following:
            //return -valA.compareTo(valB);
        }
    });
     
       for (int i = 0; i < jsonObjects.size(); i++) {
        sortedJsonArray.put(jsonObjects.get(i));
      }
        return sortedJsonArray;
    }

    /*
      This contain logic to compute the duration.
      Get the host name and type name if exist
      Get the alert
    */
   public void extractJsonData(JSONArray sortedJSonArray){
    System.out.println(sortedJSonArray);
    String id;
    String type = " ";
    String host = "";
    boolean alert;
    //ArrayList<StringBuilder> sb = new ArrayList<StringBuilder>(Collections.nCopies(sortedJSonArray.length()/2, new StringBuilder()));
     //StringBuilder sb = new StringBuilder();
    int j = 0;
     for(int i=0; i<sortedJSonArray.length(); i=i+2)
       {
            long duration = sortedJSonArray.getJSONObject(i+1).getLong("timestamp") - sortedJSonArray.getJSONObject(i).getLong("timestamp");
             id = sortedJSonArray.getJSONObject(i).getString("id");
            if(sortedJSonArray.getJSONObject(i).has("type") && sortedJSonArray.getJSONObject(i).has("host")){
               type = sortedJSonArray.getJSONObject(i).getString("type");
               host = sortedJSonArray.getJSONObject(i).getString("host");
             } else{
                      type = "xxxxxxxxxxxxxxx";
                      host = "xxxxx";
             }
   
            if(duration < 0){
               duration = Math.abs(duration);
             }
            if(duration > 4){
             alert = true;
             }
             else{
              alert = false;
             }
                   Connection con = null; 
                   Statement stmt = null; 
                   int result = 0; 
                   // SQL statement for creating a new table
                      // SQL statement for creating a new table
                String sql = "insert into event_details values(\n"
                + "    id,\n"
                + "    duration,\n"
                + "    type,\n"
                + "    host,\n"
                + "    alert \n"
                + ");";
            try { 
            Class.forName("org.hsqldb.jdbcDriver");
            con = DriverManager.getConnection( "http://hsqldb.org/"); 
            stmt = con.createStatement(); 
            result = stmt.executeUpdate(sql); 
            con.commit(); 
         }catch (Exception e) { 
         e.printStackTrace(System.out); 
         }            


         j++;
     }  

         
      //System.out.println(sb); 
       
  }


}