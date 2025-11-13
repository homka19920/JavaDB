/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.compactcode.javadb.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author dim
 */
public class DBConnect {

    private final String serverUrl;     // Адрес сервера баз данных
    private final String dbName;        // Имя базы данных
    private String user = "";           // Имя пользователя базы данных
    private String password = "";       // Пароль пользователя базы данных
    private String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"; // Используемый драйвер
    private final boolean readyToWork;  // Флаг готовности класса к работе с базы данных

    private final String sslProtocol = ";sslProtocol=TLSv1.2";

    /**
     * Конструктор для работы со встроенной системой проверки безопасности.
     *
     * @param serverUrl Адрес сервера баз данных
     * @param dbName Имя базы данных
     */
    public DBConnect(String serverUrl, String dbName) {
        this.serverUrl = serverUrl;
        this.dbName = dbName;
        readyToWork = checkConnection();
    }

    /**
     * Конструктор для работы со встроенной системой проверки безопасности и с
     * указанным драйвером.
     *
     * @param serverUrl Адрес сервера баз данных
     * @param dbName Имя базы данных
     * @param driver Используемый драйвер. По умолчанию
     * <code>com.microsoft.sqlserver.jdbc.SQLServerDriver</code>
     */
    public DBConnect(String serverUrl, String dbName, String driver) {
        this.serverUrl = serverUrl;
        this.dbName = dbName;
        this.driver = driver;

        readyToWork = checkConnection();
    }

    /**
     * Конструктор для работы с использованием имени и пароля пользователя базы
     * данных.
     *
     * @param serverUrl Адрес сервера баз данных
     * @param dbName Имя базы данных
     * @param user Имя пользователя
     * @param password Пароль
     */
    public DBConnect(String serverUrl, String dbName, String user, String password) {
        this.serverUrl = serverUrl;
        this.dbName = dbName;
        this.user = user;
        this.password = password;

        readyToWork = checkConnection();
    }

    /**
     * Конструктор для работы с использованием имени и пароля пользователя базы
     * данных и указанием драйвера.
     *
     * @param serverUrl Адрес сервера баз данных
     * @param dbName Имя базы данных
     * @param user Имя пользователя
     * @param password Пароль
     * @param driver Используемый драйвер. По умолчанию
     * <code>com.microsoft.sqlserver.jdbc.SQLServerDriver</code>
     */
    public DBConnect(String serverUrl, String dbName, String user, String password, String driver) {
        this.serverUrl = serverUrl;
        this.dbName = dbName;
        this.user = user;
        this.password = password;
        this.driver = driver;

        readyToWork = checkConnection();
    }

    /**
     * Проверяет возможность установления подключения с сервером баз данных.
     *
     * @return true - подключение успешно.
     */
    private boolean checkConnection() {
        Connection testConnection = createConnection();

        if (testConnection == null) {
            System.out.println("Can't connect to server. Check throws & url/dbName/username/password.\n"
                    + "*IntegratedSecurity option require sqljdbc_auth.dll in java.library.path.");
            return false;
        } else {
            try {
                testConnection.close();
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }
            return true;
        }
    }

    /**
     * Создаёт объект подключения.
     *
     * @return Объект подключения.
     */
    private Connection createConnection() {
        // Формирование строки подключения
        String connStr = "jdbc:sqlserver://" + this.serverUrl + ";databaseName=" + this.dbName + ";";
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
            return null;
        }

        Connection conn;
        if (this.user.equals("") && this.password.equals("")) {
            // Подключение с встроенной проверкой безопасности
            connStr += "integratedSecurity=true;";
            //trustServerCertificate=true;
            //connStr += sslProtocol;
            try {

                conn = DriverManager.getConnection(connStr);
            } catch (SQLException ex) {
                ex.printStackTrace();
                return null;
            }
        } else {
            try {
                // Подключение по имени и паролю пользователя
                conn = DriverManager.getConnection(connStr, user, password);
            } catch (SQLException ex) {
                ex.printStackTrace();
                return null;
            }
        }

        return conn;

    }

    /**
     * Выполняет любой запрос переданный в параметре
     *
     * @param query Текст запроса
     * @return ResultSet с результатами работы запроса
     */
    public ResultSet execQuery(String query) {
        if (!readyToWork) {
            System.out.println("DBC not ready to work! Abort:execQuerySelected");
            return null;
        }

        Connection conn = createConnection();

        Statement stmt;

        try {
            assert conn != null;
            stmt = conn.createStatement();
            //stmt.setFetchSize(100);

            return stmt.executeQuery(query);

        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("Something wrong... Check your query text.");
            return null;
        }
    }

    /*
     * Method to insert,Delete,Update data
     * String parameter query receives query
     * Returns boolean value
     * Throws SQLException
     */
    public int insertDeleteUpdate(String query) {

        if (!readyToWork) {
            System.out.println("DBC not ready to work! Abort:execQuerySelected");
            return 0;
        }
        Connection conn = createConnection();
        Statement stmt;
        try {
            assert conn != null;
            stmt = conn.createStatement();
            int result = stmt.executeUpdate(query);
            return result;

        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("Something wrong... Check your query text.");
            return -1;
        }

    }

    public Connection getConnect() {
        return createConnection();
    }
//    public PreparedStatement execQueryWithParam(String str){
//        if (!readyToWork) {
//            System.out.println("DBC not ready to work! Abort:execQueryWithParamSelected");
//            return null;
//        }
//
//        Connection conn = createConnection();
//
//        PreparedStatement stmt;
//
//        try {
//            assert conn != null;
//            stmt = conn.prepareStatement(user);
//            
//            stmt.setFetchSize(100);
//
//            return stmt.executeQuery(query);
//
//        } catch (SQLException e) {
//            e.printStackTrace();
//            System.out.println("Something wrong... Check your query text.");
//            return null;
//        }
//    }
}
