package com.thoughtworks.orm.core;

import com.sun.xml.internal.ws.util.StringUtils;
import com.thoughtworks.orm.annotation.PrimaryKey;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DB {
    private static Map<String, ConnectionInfo> connectionInfoMap = new HashMap<String, ConnectionInfo>();
    private static Map<String, DB> connectionMap = new HashMap<String, DB>();
    private static final String GETTER_PATTERN = "get[A-Z].*";
    private static final String SETTER_PATTERN = "set[A-Z].*";

    static {
        try {
            connectionInfoMap.put("product", new ConnectionInfo("com.mysql.jdbc.Driver",
                    "jdbc:mysql://localhost/feedback?user=sqluser&password=sqluserpw"));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    final private Connection connection;

    final private List<Integer> objectsInFromDB = new ArrayList<Integer>();

    private DB(Connection connection) {
        this.connection = connection;
    }

    public static DB connect(String dbName) throws ClassNotFoundException, SQLException {
        DB db = connectionMap.get(dbName);
        if (db == null) {
            Connection connect = connectionInfoMap.get(dbName).connect();
            connectionMap.put(dbName, new DB(connect));
        }
        return connectionMap.get(dbName);
    }

    public void save(Object obj) throws Exception {
        if(notInDb(obj)) {
            create(obj);
        } else {
            update(obj);
        }
    }

    private void update(Object obj) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException, SQLException {
        StringBuilder prepareString = new StringBuilder("update " + obj.getClass().getSimpleName() +"s set ");

        List<Method> methods = filterByPattern(obj.getClass().getDeclaredMethods(), GETTER_PATTERN);
        for(int i = 0;i < methods.size();i++) {
            if(shouldNotBeInSql(methods.get(i))) continue;

            Object value = methods.get(i).invoke(obj);
            if(!(value instanceof Integer)) {
                value = "\"" + value + "\"";
            }
            prepareString.append(getPropertyName(methods.get(i))).append("=").append(value);
            if(i != methods.size() - 1) {
                prepareString.append(",");
            }
        }

        prepareString.append(" where id = ").append(getId(obj)).append(";");
        connection.prepareStatement(prepareString.toString()).executeUpdate();
    }

    private boolean notInDb(Object obj) {
        return !objectsInFromDB.contains(obj.hashCode());
    }

    private void create(Object obj) throws Exception {
        StringBuffer prepareString = new StringBuffer("insert into " + obj.getClass().getSimpleName() +"s (");

        List<Method> methods = filterByPattern(obj.getClass().getDeclaredMethods(), GETTER_PATTERN);

        addColumnNamesTo(prepareString, methods);
        prepareString.append(") values (");

        addPlaceHolders(prepareString, methods);
        PreparedStatement preparedStatement = connection.prepareStatement(prepareString.toString());

        addValuesForPlaceHolders(obj, methods, preparedStatement);

        preparedStatement.executeUpdate();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT @@IDENTITY AS 'id'");
        if(resultSet.next()){
            obj.getClass().getMethod("setId", int.class).invoke(obj, resultSet.getInt("id"));
        }
    }

    public void delete(Object obj) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, SQLException {
        Object id = getId(obj);
        String prepareString = "delete from " + obj.getClass().getSimpleName() + "s where id=" + id + ";";
        PreparedStatement preparedStatement = connection.prepareStatement(prepareString.toString());
        preparedStatement.executeUpdate();
    }

    private Object getId(Object obj) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        return obj.getClass().getMethod("getId").invoke(obj);
    }

    private ArrayList<Method> filterByPattern(Method[] declaredMethods, String pattern) {
        ArrayList<Method> methods = new ArrayList<Method>();
        for (Method declaredMethod : declaredMethods) {
            if(declaredMethod.getName().matches(pattern)) {
                methods.add(declaredMethod);
            }
        }
        return methods;
    }

    public <T> int count(Class<T> clazz) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select count(*) as 'count' from " + clazz.getSimpleName() + "s;");
        if(resultSet.next()) {
            return resultSet.getInt("count");
        }
        return 0;
    }

    public <T> T find(Class<T> clazz, int id) throws SQLException, IllegalAccessException, InstantiationException, InvocationTargetException, NoSuchMethodException {
        Statement statement = connection.createStatement();
        statement.executeQuery("select * from " + clazz.getSimpleName() +"s where id =" + id + ";");
        ResultSet resultSet = statement.getResultSet();

        List<Method> methods = filterByPattern(clazz.getDeclaredMethods(), SETTER_PATTERN);

        if(resultSet.next()) {
            return instanceFromDB(clazz, resultSet, methods);
        }

        return null;
    }

    public <T> List<T> findAll(Class<T> clazz, String criteria) throws SQLException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException {
        Statement statement = connection.createStatement();
        statement.executeQuery("select * from " + clazz.getSimpleName() +"s where " + criteria);
        ResultSet resultSet = statement.getResultSet();

        List<Method> methods = filterByPattern(clazz.getDeclaredMethods(), SETTER_PATTERN);

        List<T> objs = new ArrayList<T>();
        while(resultSet.next()) {
            objs.add(instanceFromDB(clazz, resultSet, methods));
        }

        return objs;
    }

    private <T> T instanceFromDB(Class<T> clazz, ResultSet resultSet, List<Method> methods) throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        T t = clazz.newInstance();
        objectsInFromDB.add(t.hashCode());
        for (Method method : methods) {
            if (shouldNotBeInSql(method)) continue;

            String simpleName = method.getParameterTypes()[0].getSimpleName();

            Object value = resultSet.getClass().getMethod("get" + StringUtils.capitalize(simpleName), String.class)
                    .invoke(resultSet, getPropertyName(method));
            method.invoke(t, value);
        }
        return t;
    }

    private void addValuesForPlaceHolders(Object obj, List<Method> methods, PreparedStatement preparedStatement) throws Exception {
        int index = 1;
        for (Method method : methods) {
            if (shouldNotBeInSql(method)) continue;

            Object value = method.invoke(obj);
            if(value == null) {
                preparedStatement.setNull(index++, sqlTypeId(method.getReturnType()));
            } else {
                setValue(preparedStatement, value, index++);
            }
        }
    }

    private void addPlaceHolders(StringBuffer prepareString, List<Method> methods) {
        for (Method method : methods) {
            if (shouldNotBeInSql(method)) continue;

            prepareString.append("?,");
        }
        prepareString.replace(prepareString.length() - 1, prepareString.length(), ")");
    }

    private void addColumnNamesTo(StringBuffer prepareString, List<Method> methods) {
        for (Method method : methods) {
            if (shouldNotBeInSql(method)) continue;

            prepareString.append(getPropertyName(method) + ",");
        }
        prepareString.replace(prepareString.length() - 1, prepareString.length(), "");
    }

    private String getPropertyName(Method method) {
        return method.getName().substring(3);
    }

    @Override
    protected void finalize() throws Throwable {
        connection.close();
        super.finalize();
    }

    private int sqlTypeId(Class<?> type) throws Exception {
        if(type.equals(String.class)) {
            return Types.VARCHAR;
        } else if(type.equals(Integer.class)) {
            return Types.INTEGER;
        }
        throw new Exception("Do not know what sql type to use for " + type.getName());
    }

    private boolean shouldNotBeInSql(Method method) {
        return method.getAnnotation(PrimaryKey.class) != null
                ||!Modifier.isPublic(method.getModifiers());
    }

    private void setValue(PreparedStatement preparedStatement, Object value, int index) throws SQLException {
        if (value instanceof String) {
            preparedStatement.setString(index, (String) value);
        } else if (value instanceof Integer) {
            preparedStatement.setInt(index, (Integer) value);
        }
    }
}
