package com.thoughtworks.orm.core;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionInfo {
    private String connectString;

    public ConnectionInfo(String driverClass, String connectString) throws ClassNotFoundException {
        Class.forName(driverClass);
        this.connectString = connectString;
    }

    public Connection connect() throws SQLException {
        return DriverManager.getConnection(connectString);
    }
}
