package com.oracle;

import java.io.*;
import java.util.Properties;

public class LoadConfig {

    public static String dbUrl;
    public static String dbUser;
    public static String dbPassword;
    public static String outputDir;

    // Activa salida detallada
    public static boolean verbose = true;

    public static void loadConfig() {

        try (InputStream input = LoadConfig.class.getClassLoader().getResourceAsStream("config.dev")) {
            Properties prop = new Properties();
            prop.load(input);
            dbUrl = prop.getProperty("db.url");
            dbUser = prop.getProperty("db.user");
            dbPassword = prop.getProperty("db.password");
            outputDir = prop.getProperty("output.dir");

            File dir = new File(outputDir);
            if (!dir.exists())
                dir.mkdirs();

        } catch (IOException ex) {
            System.err.println("❌ No se pudo cargar el archivo de configuración: " + ex.getMessage());
            System.exit(1);
        }

    }

}
