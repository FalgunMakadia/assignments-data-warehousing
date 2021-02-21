// TASK 2 - INITIAL CONROLLER ENGINE - For T1 T2 T3 
// Write a controller engine using a script/program which will maintain	connection of local and remote database.
// The controller engine will contain three	embedded transactions.

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class InitialController {

	public static void main(String[] args) throws Exception {

		// For Local Connection
		String url1 = "jdbc:mysql://localhost:3306/assignment2?autoReconnect=true&useSSL=false";
		String uname1 = "root";
		String pass1 = "root";

		// For Remote Connection
		String url2 = "jdbc:mysql://localhost:3306/assignment2?autoReconnect=true&useSSL=false";
		String uname2 = "root";
		String pass2 = "root";

		Class.forName("com.mysql.cj.jdbc.Driver");

		Connection LocalConn1 = DriverManager.getConnection(url1, uname1, pass1);
		System.out.println("Local Connection 1 Established.");
		LocalConn1.setAutoCommit(false);

		Connection LocalConn2 = DriverManager.getConnection(url1, uname1, pass1);
		System.out.println("Local Connection 2 Established.");
		LocalConn2.setAutoCommit(false);

		Connection LocalConn3 = DriverManager.getConnection(url1, uname1, pass1);
		System.out.println("Local Connection 3 Established.\n");
		LocalConn3.setAutoCommit(false);

		Connection RemoteConn1 = DriverManager.getConnection(url2, uname2, pass2);
		System.out.println("Remote Connection 1 Established.\n");
		RemoteConn1.setAutoCommit(false);

		Statement s1 = LocalConn1.createStatement();
		Statement s2 = LocalConn2.createStatement();
		Statement s3 = LocalConn3.createStatement();

		// Transaction 1 Sequence 1
		String q11 = "SELECT * FROM customers WHERE customer_zip_code_prefix = 64100";
		ResultSet rs11 = s1.executeQuery(q11);
		rs11.next();
		String Data11 = "\nCustomer ID:" + rs11.getString(1) + "\nCustomer Unique ID:" + rs11.getString(2)
				+ "\nZIP Code:" + rs11.getInt(3) + "\nCity:" + rs11.getString(4) + "\nState:" + rs11.getString(5);
		String cuid11 = rs11.getString(2);
		System.out.println("Data after T1 S1: " + Data11);
		System.out.println("\n");

		// Transaction 2 Sequence 1
		String q21 = "SELECT * FROM customers WHERE customer_zip_code_prefix = 64100";
		ResultSet rs21 = s2.executeQuery(q21);
		rs21.next();
		String Data21 = "\nCustomer ID:" + rs21.getString(1) + "\nCustomer Unique ID:" + rs21.getString(2)
				+ "\nZIP Code:" + rs21.getInt(3) + "\nCity:" + rs21.getString(4) + "\nState:" + rs21.getString(5);
		String cuid21 = rs21.getString(2);
		System.out.println("Data after T2 S1: " + Data21);
		System.out.println("\n");

		// Transaction 1 Sequence 2
		String q12 = "UPDATE customers SET customer_city = 'T1 City' WHERE customer_unique_id = '" + cuid11 + "'";
		int count12 = s1.executeUpdate(q12);
		System.out.println("Data after T1 S2: " + count12 + " row/s affected!");
		System.out.println("\n");

		// Transaction 3 Sequence 2
		String q32 = "SELECT * FROM customers WHERE customer_zip_code_prefix = 64100";
		ResultSet rs32 = s3.executeQuery(q32);
		rs32.next();
		String Data32 = "\nCustomer ID:" + rs32.getString(1) + "\nCustomer Unique ID:" + rs32.getString(2)
				+ "\nZIP Code:" + rs32.getInt(3) + "\nCity:" + rs32.getString(4) + "\nState:" + rs32.getString(5);
		String cuid32 = rs32.getString(2);
		System.out.println("Data after T3 S2: " + Data32);
		System.out.println("\n");

		// Transaction 2 Sequence 3
		String q23 = "UPDATE customers SET customer_city = 'T2 City' WHERE customer_unique_id = '" + cuid21 + "'";
		int count23 = s2.executeUpdate(q23);
		System.out.println("Data after T2 S3: " + count23 + " row/s affected!");
		System.out.println("\n");

		// Transaction 3 Sequence 3
		String q33 = "UPDATE customers SET customer_city = 'T3 City' WHERE customer_unique_id = '" + cuid32 + "'";
		int count33 = s3.executeUpdate(q33);
		System.out.println("Data after T3 S3: " + count33 + " row/s affected!");
		System.out.println("\n");

		LocalConn1.commit();
		LocalConn2.commit();
		LocalConn3.commit();
		RemoteConn1.commit();

		s1.close();
		s2.close();
		s3.close();

		LocalConn1.close();
		LocalConn2.close();
		LocalConn3.close();
		RemoteConn1.close();

	}

}
