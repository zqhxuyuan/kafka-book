package base.concurrent.future;

import java.sql.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class JDBCFutureExample {

    public static void main(String[] args) {
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        try(Connection conn = DriverManager.getConnection("jdbc:h2:/tmp/h2", "sa", "");
            Statement stmt = conn.createStatement();
        ){
            String querySQL = "SELECT id, first, last, age FROM test";

            ExecutorService threadPool = Executors.newSingleThreadExecutor();
            Future<ResultSet> future = threadPool.submit(new Callable<ResultSet>() {
                public ResultSet call() throws Exception {
                    Thread.sleep(5000);
                    ResultSet rs = stmt.executeQuery(querySQL);
                    return rs;
                }
            });

            ResultSet rs = future.get();
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
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}