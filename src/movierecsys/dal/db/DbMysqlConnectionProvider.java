/**
 * Author: Carlo De Leon
 * Version: 1.0
 */

package movierecsys.dal.db;

import movierecsys.dal.intereface.IDbConnectionProvider;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DbMysqlConnectionProvider implements IDbConnectionProvider {

    protected String host;
    protected String user;
    protected String password;
    protected String database;
    protected int port = 3306;

    protected Connection connection;

    public DbMysqlConnectionProvider() {
    }

    @Override
    public void connect() {
        try {
            // Connect to the database.
            connection = DriverManager.getConnection(String.format("jdbc:mysql://%s:%d/%s", getHost(), getPort(), getDatabase()), getUser(), getPassword());
        } catch (SQLException e) {
            System.out.println(String.format("MySQL connection exception: %s", e.getMessage()));
        }
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public String getDatabase() {
        return database;
    }


    @Override
    public String getHost() {
        return host;
    }

    @Override
    public String getUser() {
        return user;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public void setDatabase(String database) {
        if (database.isEmpty()) return;
        this.database = database;
    }

    @Override
    public void setHost(String host) {
        if (host.isEmpty()) return;
        this.host = host;
    }

    @Override
    public void setUser(String user) {
        if (user.isEmpty()) return;
        this.user = user;
    }

    @Override
    public void setPassword(String password) {
        if (password.isEmpty()) return;
        this.password = password;
    }

    @Override
    public void setPort(int port) {
        if (port <= 0) return;
        this.port = port;
    }
}
