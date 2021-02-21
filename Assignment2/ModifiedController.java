// TASK 2 - MODIFIED CONROLLER ENGINE - For Performing 2 Distributed Transactions
// Transactions T1 and T2 are performing 8 Operations each (3 on Local site and 5 on Remote site) i.e. Total 16 INSERT/UPDATE/DELETE Operations

import java.security.SecureRandom;
import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ModifiedController {

    static final String STR = "0123456789abcdefghijklmnopqrstuvwxyz";
    static SecureRandom random = new SecureRandom();

    // Method for creating Random Alpha-Numeric sequence for INSERT operations
    static String randomString(int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++)
            sb.append(STR.charAt(random.nextInt(STR.length())));
        return sb.toString();
    }

    // For Finding if queried Table is at Remote or Local site
    public static Statement findLocation(Statement s1, Statement s2, String realQuery) throws SQLException {

        // Fetching Table names from Local Data Dictionary at Local site
        String q = "SELECT table_name FROM ldd";
        ResultSet rs = s2.executeQuery(q);

        // Since there are only 2 sites, no need to check for Remote site because if the
        // table is not at Local site then it will be at Remote site.

        String regex = "";

        while (rs.next()) {
            regex = regex.concat(rs.getString(1) + "|");
        }

        StringBuffer strbuffer = new StringBuffer(regex);
        strbuffer.deleteCharAt(strbuffer.length() - 1);

        // Identifying which table is it that the realQuery is querying (using Regular
        // Expression)
        Pattern p = Pattern.compile("(" + strbuffer + ")");
        Matcher m = p.matcher(realQuery);

        // if m.find() is true then identified table is at Local site otherwise it is
        // place at remote site.
        if (m.find()) {
            return s2;
        } else {
            return s1;
        }

    }

    public static void main(String[] args) throws Exception {

        // For Remote Connection
        String url1 = "jdbc:mysql://35.200.191.47:3306/remote-db-a2?autoReconnect=true&useSSL=false";
        String uname1 = "root";
        String pass1 = "root";

        // For Local Connection
        String url2 = "jdbc:mysql://localhost:3306/assignment2?autoReconnect=true&useSSL=false";
        String uname2 = "root";
        String pass2 = "root";

        Class.forName("com.mysql.cj.jdbc.Driver");

        Connection remoteConn = DriverManager.getConnection(url1, uname1, pass1);
        System.out.println("Remote Connection Established!");
        remoteConn.setAutoCommit(false);
        Statement s1 = remoteConn.createStatement();

        Connection localConn = DriverManager.getConnection(url2, uname2, pass2);
        System.out.println("Local Connection Established!\n\n");
        localConn.setAutoCommit(false);
        Statement s2 = localConn.createStatement();

        Statement stmt;

        // Transaction 1
        System.out.println("======================== TRANSACTION 1 ========================\n");

        // Queries for Transaction 1
        String t1Query1 = "UPDATE order_payments SET payment_installments = 2 "
                + "WHERE order_id = '2a8dda5bf0b2fc7f28cc417b83441629'";
        String t1Query2 = "UPDATE order_items " + "SET price = 39.99 "
                + "WHERE order_id = '00ac2a1ec784f71feeb7ba1e5959750d'";
        String t1Query3 = "UPDATE sellers SET seller_zip_code_prefix = 30500 "
                + "WHERE seller_id = '1c742ac33582852aaf3bcfbf5893abcf'";
        String t1Query4 = "INSERT INTO order_review "
                + "(`review_id`,`order_id`,`review_score`,`review_comment_title`,`review_comment_message`,`review_creation_date`,`review_answer_timestamp`) "
                + "VALUES ('" + randomString(32) + "', '" + randomString(32)
                + "', 5, 'nice', 'muito bom', '2/05/2018 0:00', '2/21/2018 12:39')";
        String t1Query5 = "UPDATE customers SET customer_zip_code_prefix = 11001 "
                + "WHERE customer_unique_id = '0f4ca3957391f0293d4d691029e52225'";
        String t1Query6 = "DELETE FROM products " + "WHERE product_id = '287d083ef43e221d8558ac223556de34'";
        String t1Query7 = "UPDATE geolocation SET geolocation_zip_code_prefix = 1258 "
                + "WHERE geolocation_number = 125";
        String t1Query8 = "DELETE FROM orders " + "WHERE order_id = '50c67fc0ce3ee7959b7dfeeabfbe6b78'";

        stmt = findLocation(s1, s2, t1Query1);
        int t1r1 = stmt.executeUpdate(t1Query1);
        System.out.println("Query 1 : " + t1r1 + " row(s) affected in 'order_payments' table - at Remote site!");

        stmt = findLocation(s1, s2, t1Query2);
        int t1r2 = stmt.executeUpdate(t1Query2);
        System.out.println("Query 2 : " + t1r2 + " row(s) affected in 'order_items' table - at Remote site!");

        stmt = findLocation(s1, s2, t1Query3);
        int t1r3 = stmt.executeUpdate(t1Query3);
        System.out.println("Query 3 : " + t1r3 + " row(s) affected in 'sellers' table - at Local site!");

        stmt = findLocation(s1, s2, t1Query4);
        int t1r4 = stmt.executeUpdate(t1Query4);
        System.out.println("Query 4 : " + t1r4 + " row(s) affected in 'order_review' table - at Remote site!");

        stmt = findLocation(s1, s2, t1Query5);
        int t1r5 = stmt.executeUpdate(t1Query5);
        System.out.println("Query 5 : " + t1r5 + " row(s) affected in 'customers' table - at Local site!");

        stmt = findLocation(s1, s2, t1Query6);
        int t1r6 = stmt.executeUpdate(t1Query6);
        System.out.println("Query 6 : " + t1r6 + " row(s) affected in 'products' table - at Remote site!");

        stmt = findLocation(s1, s2, t1Query7);
        int t1r7 = stmt.executeUpdate(t1Query7);
        System.out.println("Query 7 : " + t1r7 + " row(s) affected in 'geolocation' table - at Local site!");

        stmt = findLocation(s1, s2, t1Query8);
        int t1r8 = stmt.executeUpdate(t1Query8);
        System.out.println("Query 8 : " + t1r8 + " row(s) affected in 'orders' table - at Remote site!");

        // Commit Transaction 1
        remoteConn.commit();
        localConn.commit();

        System.out.println("<<< Transaction 1 Committed! >>>\n");
        System.out.println("===============================================================\n\n");

        // Transaction 2
        System.out.println("======================== TRANSACTION 2 ========================\n");

        // Queries for Transaction 2
        String t2Query1 = "UPDATE customers " + "SET customer_city = 'presidente venceslau', customer_state = 'SP' "
                + "WHERE customer_unique_id = '1bef0b73e706c035cfb4755cdc594afc'";
        String t2Query2 = "DELETE FROM order_payments " + "WHERE order_id = '31d2d973ab3772f718de008bfacc3d07'";
        String t2Query3 = "DELETE FROM orders " + "WHERE order_id = '6245ae0abd9de1ae647b5da78c0ea69b'";
        String t2Query4 = "UPDATE order_items SET price = 1199.99 " + "WHERE order_number = 654";
        String t2Query5 = "UPDATE geolocation " + "SET geolocation_zip_code_prefix = 4500, geolocation_state = 'PE' "
                + "WHERE geolocation_number = 542";
        String t2Query6 = "UPDATE sellers " + "SET seller_city = 'sao paulo' "
                + "WHERE seller_id = '40536e7ca18e1bce252828e5876466cc'";
        String t2Query7 = "INSERT INTO products "
                + "(`product_id`,`product_category_name`,`product_name_lenght`,`product_description_lenght`,`product_photos_qty`,`product_weight_g`,`product_length_cm`,`product_height_cm`,`product_width_cm`) "
                + "VALUES ('" + randomString(32) + "', 'eletrodomesticos', 8, 18, 1, 750, 2, 1, 3)";
        String t2Query8 = "UPDATE order_review SET review_comment_title = 'recomendado' "
                + "WHERE review_id = '6lhasasvc3bhwnzpsblo13l54f6acnti'";

        stmt = findLocation(s1, s2, t2Query1);
        int t2r1 = stmt.executeUpdate(t2Query1);
        System.out.println("Query 1 : " + t2r1 + " row(s) affected in 'customers' table - at Local site!");

        stmt = findLocation(s1, s2, t2Query2);
        int t2r2 = stmt.executeUpdate(t2Query2);
        System.out.println("Query 2 : " + t2r2 + " row(s) affected in 'order_payments' table - at Remote site!");

        stmt = findLocation(s1, s2, t2Query3);
        int t2r3 = stmt.executeUpdate(t2Query3);
        System.out.println("Query 3 : " + t2r3 + " row(s) affected in 'orders' table - at Remote site!");

        stmt = findLocation(s1, s2, t2Query4);
        int t2r4 = stmt.executeUpdate(t2Query4);
        System.out.println("Query 4 : " + t2r4 + " row(s) affected in 'order_items' table - at Remote site!");

        stmt = findLocation(s1, s2, t2Query5);
        int t2r5 = stmt.executeUpdate(t2Query5);
        System.out.println("Query 5 : " + t2r5 + " row(s) affected in 'geolocation' table - at Local site!");

        stmt = findLocation(s1, s2, t2Query6);
        int t2r6 = stmt.executeUpdate(t2Query6);
        System.out.println("Query 6 : " + t2r6 + " row(s) affected in 'sellers' table - at Local site!");

        stmt = findLocation(s1, s2, t2Query7);
        int t2r7 = stmt.executeUpdate(t2Query7);
        System.out.println("Query 7 : " + t2r7 + " row(s) affected in 'products' table - at Remote site!");

        stmt = findLocation(s1, s2, t2Query8);
        int t2r8 = stmt.executeUpdate(t2Query8);
        System.out.println("Query 8 : " + t2r8 + " row(s) affected in 'order_review' table - at Remote site!");

        // Commit Transaction 2
        remoteConn.commit();
        localConn.commit();

        System.out.println("<<< Transaction 2 Committed! >>>\n");
        System.out.println("===============================================================\n\n");

        stmt.close();
        s1.close();
        s2.close();

        remoteConn.close();
        localConn.close();

    }

}