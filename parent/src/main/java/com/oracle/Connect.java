package com.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Connect extends LoadConfig {

    public static Connection getConnection() {

        Connection conn = null;

        loadConfig();
        try {

            conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
            System.out.println("OK: Conectado a Oracle correctamente.");

        } catch (SQLException e) {
            System.err.println("FAIL: Error de conexión: " + e.getMessage());
        }
        return conn;
    }

}
