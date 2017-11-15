
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;


public class ScanTable{

   public static void main(String args[]) throws IOException{

      // Instantiating Configuration class
      Configuration config = HBaseConfiguration.create();

      // Instantiating HTable class
      HTable table = new HTable(config, "emp");

      // Instantiating the Scan class
      Scan scan = new Scan();

      // Scanning the required columns
      //scan.addColumn(Bytes.toBytes("id"), Bytes.toBytes("firstid"));
      //scan.addColumn(Bytes.toBytes("name"), Bytes.toBytes("firstname"));

      // Getting the scan result
      ResultScanner scanner = table.getScanner(scan);

      // Reading values from scan result
      for (Result row : scanner) {
    	  byte[] valueBytes = row.getValue(Bytes.toBytes("id"), Bytes.toBytes("secondid"));
    	  System.out.println('\t' + Bytes.toString(valueBytes));
    	}
      //closing the scanner
      scanner.close();
   }
}