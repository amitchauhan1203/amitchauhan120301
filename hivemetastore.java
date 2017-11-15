
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
public class hivemetastore {

    /**

     * @param args

     */

    public static void main(String[] args) {

        // TODO Auto-generated method stub
         try {
             String driverName = "com.mysql.jdbc.Driver";
             Class.forName(driverName);
             } catch (ClassNotFoundException e) {
               // TODO Auto-generated catch block
               e.printStackTrace();
               System.exit(1);
             }
         try {
             Connection con = DriverManager.getConnection("jdbc:mysql://127.0.0.1/metastore","hive","cloudera");
             Statement stmt = con.createStatement();
             String sql = "	select t3.LOCATION  from TBLS t1 ,PARTITIONS t2,SDS t3 where t1.TBL_ID=t2.TBL_ID and t2.SD_ID=t3.SD_ID; ";
             System.out.println("Running: " + sql);
             ResultSet res = stmt.executeQuery(sql);
             while (res.next()) {
                 System.out.println(res.getString(1)  );
               }
             //System.out.println("Query execution complete");
         
         
         String cmd = "hadoop fs -cat /home/cloudera/Desktop/file/*";

         Runtime run = Runtime.getRuntime();

         Process pr = run.exec(cmd);

         pr.waitFor();

         BufferedReader buf = new BufferedReader(new InputStreamReader(pr.getInputStream()));

         String line = "";

         while ((line=buf.readLine())!=null) {

         System.out.println(line);
         } 
         }catch (Exception e) {
             // TODO Auto-generated catch block
             e.printStackTrace();
         
         }
  }
}