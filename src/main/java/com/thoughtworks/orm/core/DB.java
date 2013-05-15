package com.thoughtworks.orm.core;

import com.sun.xml.internal.ws.util.StringUtils;
import com.thoughtworks.orm.annotation.BelongsTo;
import com.thoughtworks.orm.annotation.HasMany;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
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

    public static void init(String configFile) throws DocumentException, ClassNotFoundException {
        InputStream inputStream = DB.class.getClassLoader().getResourceAsStream(configFile);
        SAXReader reader = new SAXReader();
        Document doc = reader.read(inputStream);
        for (Object o : doc.getRootElement().elements("db")) {
            Element element = (Element) o;
            String name = element.attribute("name").getValue();
            String driver = element.element("driver").getText();
            String connectString = element.element("connectString").getText();
            String username = element.element("username").getText();
            String password = element.element("password").getText();

            connectionInfoMap.put(name, new ConnectionInfo(driver, connectString + "?user=" + username + "&password=" + password));
        }
    }

    ////////////////////////////////////////////////////////////////

    final private Connection connection;
    final private List<Integer> objectsIdFromDB = new ArrayList<Integer>();
    final private List<Object> objectsCache = new ArrayList<Object>();
    private int callDepth = 0;

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

    public void save(Object obj, Object... belongsTos) throws Exception {
        if(notInDb(obj)) {
            create(obj, belongsTos);
        } else {
            update(obj, belongsTos);
        }
        saveAssociations(obj);
    }

    private void saveAssociations(Object obj) throws Exception {
        for (Method declaredMethod : filterByPattern(obj.getClass().getDeclaredMethods(), GETTER_PATTERN)) {
            if(markedWithAnnotation(obj.getClass(), declaredMethod, HasMany.class)) {
                for (Object association : (List)declaredMethod.invoke(obj)) {
                    save(association, obj);
                }
            }
        }
    }

    private void update(Object obj,  Object[] belongsTos) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException, SQLException, NoSuchFieldException {
        StringBuilder prepareString = new StringBuilder("update " + obj.getClass().getSimpleName() +"s set ");

        List<Method> methods = filterByPattern(obj.getClass().getDeclaredMethods(), GETTER_PATTERN);
        for(int i = 0;i < methods.size();i++) {
            if(shouldNotBeInSql(obj.getClass(), methods.get(i))) continue;

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
        return !objectsIdFromDB.contains(obj.hashCode());
    }

    private void create(Object obj, Object[] belongsTos) throws Exception {
        StringBuffer prepareString = new StringBuffer("insert into " + obj.getClass().getSimpleName() +"s (");

        List<Method> methods = filterByPattern(obj.getClass().getDeclaredMethods(), GETTER_PATTERN);

        addColumnNamesTo(prepareString, methods, obj.getClass());
        addBelongsToColumnNamesTo(prepareString, belongsTos);
        prepareString.append(") values (");

        addPlaceHolders(prepareString, methods, obj.getClass(), belongsTos.length);
        PreparedStatement preparedStatement = connection.prepareStatement(prepareString.toString());

        int index = addValuesForPlaceHolders(obj, methods, preparedStatement);
        addBelongsToValuesTo(preparedStatement, belongsTos, index);

        preparedStatement.executeUpdate();

        populateId(obj);
    }

    private void addBelongsToValuesTo(PreparedStatement preparedStatement, Object[] belongsTos, int index) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, SQLException {
        for (Object belongsTo : belongsTos) {
            preparedStatement.setInt(index++, (Integer)getId(belongsTo));
        }
    }

    private void addBelongsToColumnNamesTo(StringBuffer prepareString, Object[] belongsTos) {
        for(int i = 0;i < belongsTos.length;i++) {
            prepareString.append(",");
            prepareString.append(belongsTos[i].getClass().getSimpleName() + "_id");
        }
    }

    private void populateId(Object obj) throws SQLException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
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

    public <T> T find(Class<T> clazz, int id) throws SQLException, IllegalAccessException, InstantiationException, InvocationTargetException, NoSuchMethodException, NoSuchFieldException, ClassNotFoundException {
        increaseDepth();

        Statement statement = connection.createStatement();
        statement.executeQuery("select * from " + clazz.getSimpleName() +"s where id =" + id + ";");
        ResultSet resultSet = statement.getResultSet();

        List<Method> methods = filterByPattern(clazz.getDeclaredMethods(), SETTER_PATTERN);

        T t = null;
        if(resultSet.next()) {
            t = instanceFromDBOrCache(clazz, resultSet, methods);
        }

        tryEndSearchSession();
        return t;
    }

    private void tryEndSearchSession() {
        if(--callDepth == 0) {
            objectsCache.clear();
        }
    }

    private void increaseDepth() {
        callDepth++;
    }

    private <T> void retrieveHasManyAssociations(Class<T> clazz, T t) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, SQLException, NoSuchFieldException, InstantiationException, ClassNotFoundException {
        Object id = getId(t);
        String foreignKey = clazz.getSimpleName() + "_id";
        for (Method method : filterByPattern(clazz.getDeclaredMethods(), SETTER_PATTERN)) {
            if(markedWithAnnotation(clazz, method, HasMany.class)) {
                ParameterizedType pt = (ParameterizedType) method.getGenericParameterTypes()[0];
                // pt.getActualTypeArguments()[0].toString() will be "class #{some qualified name}", so 6 is to skip the leading "class "
                Class associationClazz = Class.forName(pt.getActualTypeArguments()[0].toString().substring(6));

                method.invoke(t, findAll(associationClazz, foreignKey + " = " + id));
            }
        }
    }

    private <T> void populateBelongsToAssociation(Class<T> clazz, T t, ResultSet resultSet) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, NoSuchFieldException, ClassNotFoundException, SQLException, InstantiationException {
        for (Method method : filterByPattern(clazz.getDeclaredMethods(), SETTER_PATTERN)) {
            if(markedWithAnnotation(clazz, method, BelongsTo.class)) {
                Class<?> belongsToClass = method.getParameterTypes()[0];
                Object belongsToObj = find(belongsToClass, resultSet.getInt(belongsToClass.getSimpleName() + "_id"));
                method.invoke(t, belongsToObj);
            }
        }
    }

    public <T> List<T> findAll(Class<T> clazz, String criteria) throws SQLException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException, NoSuchFieldException, ClassNotFoundException {
        increaseDepth();

        Statement statement = connection.createStatement();
        if(!criteria.isEmpty()) criteria = " where " + criteria;

        statement.executeQuery("select * from " + clazz.getSimpleName() +"s" + criteria);
        ResultSet resultSet = statement.getResultSet();

        List<Method> methods = filterByPattern(clazz.getDeclaredMethods(), SETTER_PATTERN);

        List<T> objs = new ArrayList<T>();
        while(resultSet.next()) {
            objs.add(instanceFromDBOrCache(clazz, resultSet, methods));
        }

        tryEndSearchSession();
        return objs;
    }

    private <T> T instanceFromDBOrCache(Class<T> clazz, ResultSet resultSet, List<Method> methods) throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, NoSuchFieldException, ClassNotFoundException, SQLException {
        T objFromCache = findFromCache(clazz, resultSet.getInt("id"));
        if(objFromCache != null) return objFromCache;

        T t = clazz.newInstance();
        objectsIdFromDB.add(t.hashCode());
        for (Method method : methods) {
            if (shouldNotBeInSql(clazz, method)) continue;

            String simpleName = method.getParameterTypes()[0].getSimpleName();

            Object value = resultSet.getClass().getMethod("get" + StringUtils.capitalize(simpleName), String.class)
                    .invoke(resultSet, getPropertyName(method));
            method.invoke(t, value);
        }
        objectsCache.add(t);

        retrieveHasManyAssociations(clazz, t);
        populateBelongsToAssociation(clazz, t, resultSet);
        return t;
    }

    private <T> T findFromCache(Class<T> clazz, int id) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        for (Object o : objectsCache) {
            if(o.getClass().equals(clazz) && getId(o).equals(id)) {
                return (T)o;
            }
        }
        return null;
    }

    private int addValuesForPlaceHolders(Object obj, List<Method> methods, PreparedStatement preparedStatement) throws Exception {
        int index = 1;
        for (Method method : methods) {
            if (shouldNotBeInSql(obj.getClass(), method)) continue;

            Object value = method.invoke(obj);
            if(value == null) {
                preparedStatement.setNull(index++, sqlTypeId(method.getReturnType()));
            } else {
                setValue(preparedStatement, value, index++);
            }
        }
        return index;
    }

    private void addPlaceHolders(StringBuffer prepareString, List<Method> methods, Class<?> clazz, int belongsTo) throws NoSuchMethodException, NoSuchFieldException {
        for (Method method : methods) {
            if (shouldNotBeInSql(clazz, method)) continue;

            prepareString.append("?,");
        }
        for(int i = 0;i < belongsTo;i++) {
            prepareString.append("?,");
        }
        prepareString.replace(prepareString.length() - 1, prepareString.length(), ")");
    }

    private void addColumnNamesTo(StringBuffer prepareString, List<Method> methods, Class<?> clazz) throws NoSuchMethodException, NoSuchFieldException {
        for (Method method : methods) {
            if (shouldNotBeInSql(clazz, method)) continue;

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

    private boolean shouldNotBeInSql(Class<?> clazz, Method method) throws NoSuchFieldException, NoSuchMethodException {
        if (!Modifier.isPublic(method.getModifiers()))
            return true;
        if (markedWithAnnotation(clazz, method, HasMany.class)) return true;
        if (markedWithAnnotation(clazz, method, BelongsTo.class)) return true;
        return false;
    }

    private boolean markedWithAnnotation(Class<?> clazz, Method method, Class annotation) throws NoSuchFieldException, NoSuchMethodException {
        String propertyName = method.getName().substring(3);
        Field field = clazz.getDeclaredField(StringUtils.decapitalize(propertyName));

        if (clazz.getMethod("set" + propertyName, field.getType()).getAnnotation(annotation) != null
            || clazz.getMethod("get" + propertyName).getAnnotation(annotation) != null
            || field.getAnnotation(annotation) != null)
            return true;
        return false;
    }

    private void setValue(PreparedStatement preparedStatement, Object value, int index) throws Exception {
        if (value instanceof String) {
            preparedStatement.setString(index, (String) value);
        } else if (value instanceof Integer) {
            preparedStatement.setInt(index, (Integer) value);
        } else {
            throw new Exception("Do not know how to set value for type: " + value.getClass().getSimpleName());
        }
    }
}
