import java.nio.ShortBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
public class hive {

    /**

     * @param args

     */

    public static void main(String[] args) {

        // TODO Auto-generated method stub
         try {
             String driverName = "org.apache.hive.jdbc.HiveDriver";
             Class.forName(driverName);
             } catch (ClassNotFoundException e) {
               // TODO Auto-generated catch block
               e.printStackTrace();
               System.exit(1);
             }
         try {
             Connection con = DriverManager.getConnection("jdbc:hive2://"+"localhost" + ":" +"10000" + "/default", "cloudera", "cloudera");
             Statement stmt = con.createStatement();
             String sql = "select * from orders limit 5";
             //System.out.println("Running: " + sql);
             ResultSet res = stmt.executeQuery(sql);
             int j=res.getMetaData().getColumnCount();
             
             StringBuffer header= new StringBuffer();
             
             for (int i=1;i<=j;i++)
        	 {
            	 
               
             header.append(res.getMetaData().getColumnName(i).substring(res.getMetaData().getColumnName(i).indexOf(".")+1)+"|");
        	 }
             System.out.println(header);
             while (res.next()) {
            	 
            	 StringBuffer s=new StringBuffer();
            	 for (int i=1;i<=j;i++)
            	 {
            		 //System.out.println(i+":"+res.getString(i));
            		 if(i<j)
            		 s.append(res.getString(i)+"|");
            		 else
            			 s.append(res.getString(i));
            		 
            		
            		 
            	 }
                 System.out.println(s );
               }
             //System.out.println("Query execution complete");
         } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
  }
}