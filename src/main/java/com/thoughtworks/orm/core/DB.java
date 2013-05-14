package com.thoughtworks.orm.core;

import com.thoughtworks.orm.annotation.PrimaryKey;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class DB {
    private static Map<String, ConnectionInfo> connectionInfoMap = new HashMap<String, ConnectionInfo>();
    private static Map<String, DB> connectionMap = new HashMap<String, DB>();

    static {
        try {
            connectionInfoMap.put("product", new ConnectionInfo("com.mysql.jdbc.Driver",
                    "jdbc:mysql://localhost/feedback?user=sqluser&password=sqluserpw"));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    final private Connection connection;

    private DB(Connection connection) {
        this.connection = connection;
    }

    public static DB connect(String dbName) throws ClassNotFoundException, SQLException {
        DB db = connectionMap.get("dbName");
        if (db == null) {
            Connection connect = connectionInfoMap.get(dbName).connect();
            connectionMap.put("dbName", new DB(connect));
        }
        return connectionMap.get("dbName");
    }

    public void save(Object obj) throws Exception {
        StringBuffer prepareString = new StringBuffer("insert into " + obj.getClass().getSimpleName() +"s (");

        Method[] methods = obj.getClass().getDeclaredMethods();

        addColumnNamesTo(prepareString, methods);
        prepareString.append(") values (");

        addPlaceHolders(prepareString, methods);
        PreparedStatement preparedStatement = connection.prepareStatement(prepareString.toString());

        addValuesForPlaceHolders(obj, methods, preparedStatement);

        preparedStatement.executeUpdate();
    }

    @Override
    protected void finalize() throws Throwable {
        connection.close();
        super.finalize();
    }

    private void addValuesForPlaceHolders(Object obj, Method[] methods, PreparedStatement preparedStatement) throws Exception {
        int index = 1;
        for (Method method : methods) {
            if (shouldBeInSql(method)) continue;

            Object value = method.invoke(obj);
            if(value == null) {
                preparedStatement.setNull(index++, sqlTypeId(method.getReturnType()));
            } else {
                setValue(preparedStatement, value, index++);
            }
        }
    }

    private void addPlaceHolders(StringBuffer prepareString, Method[] methods) {
        for (Method method : methods) {
            if (shouldBeInSql(method)) continue;

            prepareString.append("?,");
        }
        prepareString.replace(prepareString.length() - 1, prepareString.length(), ")");
    }

    private void addColumnNamesTo(StringBuffer prepareString, Method[] methods) {
        for (Method method : methods) {
            if (shouldBeInSql(method)) continue;

            prepareString.append(method.getName().substring(3) + ",");
        }
        prepareString.replace(prepareString.length() - 1, prepareString.length(), "");
    }

    private int sqlTypeId(Class<?> type) throws Exception {
        if(type.equals(String.class)) {
            return Types.VARCHAR;
        } else if(type.equals(Integer.class)) {
            return Types.INTEGER;
        }
        throw new Exception("Do not know what sql type to use for " + type.getName());
    }

    private boolean shouldBeInSql(Method method) {
        return method.getAnnotation(PrimaryKey.class) != null
                ||!Modifier.isPublic(method.getModifiers())
                ||!method.getName().matches("get[A-Z].*");
    }

    private void setValue(PreparedStatement preparedStatement, Object value, int index) throws SQLException {
        if (value instanceof String) {
            preparedStatement.setString(index, (String) value);
        } else if (value instanceof Integer) {
            preparedStatement.setInt(index, (Integer) value);
        }
    }
}
