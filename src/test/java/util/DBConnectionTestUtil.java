package util;

import java.sql.*;

public class DBConnectionTestUtil {
    private Connection connect = null;
    private Statement statement = null;
    private ResultSet resultSet = null;

    public DBConnectionTestUtil() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connect = DriverManager
                    .getConnection("jdbc:mysql://localhost/feedback?"
                            + "user=sqluser&password=sqluserpw");

        } catch (Exception e) {

        }
    }

    public void executeUpdate(String sql) {
        try {
            connect.prepareStatement(sql).executeUpdate();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public ResultSet executeQuery(String sql) {
        try {
            statement = connect.createStatement();
            statement.execute(sql);
            return statement.getResultSet();
        } catch (Exception e) {
            System.out.println(e);
            return null;
        }
    }

    public void close() {
        try {
            if (resultSet != null) {
                resultSet.close();
            }

            if (statement != null) {
                statement.close();
            }

            if (connect != null) {
                connect.close();
            }
        } catch (Exception e) {

        }
    }

}
