package base.concurrent.future;

import java.sql.*;

public class JDBCExample {
    static final String JDBC_DRIVER = "org.h2.Driver";
    static final String DB_URL = "jdbc:h2:/tmp/h2";
    static final String USER = "sa";
    static final String PASS = "";

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        try{
            Class.forName("org.h2.Driver");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);

            stmt = conn.createStatement();

            String tableSQL = "CREATE TABLE test " +
                    "(id INTEGER not NULL, " +
                    " first VARCHAR(255), " +
                    " last VARCHAR(255), " +
                    " age INTEGER)";
            stmt.executeUpdate(tableSQL);

            String insertSQL = "INSERT INTO test VALUES (12345, 'z', 'qh', 30)";
            stmt.executeUpdate(insertSQL);

            String querySQL = "SELECT id, first, last, age FROM test";
            ResultSet rs = stmt.executeQuery(querySQL);
            while(rs.next()){
                int id  = rs.getInt("id");
                int age = rs.getInt("age");
                String first = rs.getString("first");
                String last = rs.getString("last");

                System.out.print("ID: " + id);
                System.out.print(", Age: " + age);
                System.out.print(", First: " + first);
                System.out.println(", Last: " + last);
            }
            rs.close();
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            try{
                if(stmt!=null)
                    conn.close();
            }catch(SQLException se){}
            try{
                if(conn!=null)
                    conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
    }
}