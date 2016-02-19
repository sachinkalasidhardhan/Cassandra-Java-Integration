package casstest;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;


public class cassandra_tester {


	public static void main(String[] args) {
		
		System.out.print("***** Cassandra - Java Connection Tester ******");
		Cluster cluster;
		Session session;
		// Connect to the cluster and keyspace "ecommerce" 127.0.0.1:9042/9160
		cluster = Cluster.builder().addContactPoint("localhost").build();	
		session = cluster.connect("ecommerce");
		
		// Use insert statement
		System.out.println("Inserting into Cassandra Database...");
		session.execute("INSERT INTO products (pdt_id, cat_id, pdt_name, pdt_desc, price, shipping) VALUES (005,104, 'Danby 0.7 cu. ft. Microwave Oven', 'Capacity of 0.7 cu. ft.10 different power levels', 54.00, 'Expedited')");
		
		// Use select statement
	String pdtid = null, pdtname = null, pdtdesc = null;
	float price = 0;
	
		
		// Use select statement
		ResultSet results = session.execute("SELECT * FROM products where pdt_id = 5");
		
		for (Row row : results) {
			pdtid = Integer.toString(row.getInt("pdt_id"));
			pdtname = row.getString("pdt_name");
			pdtdesc = row.getString("pdt_desc");
			price = row.getFloat("price");
			
			
		System.out.println("Product ID: " + pdtid);
		System.out.println("Name: " + pdtname);
		System.out.println("Description: " + pdtdesc);
		System.out.println("Price: "+ price);
		}
		
		
		// Clean up the connection by closing it
		cluster.close();
}
}
