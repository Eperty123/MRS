/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package movierecsys.dal.db;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import movierecsys.dal.intereface.IDbConnectionProvider;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author pgn
 */
public class DbMSSQLConnectionProvider implements IDbConnectionProvider {

    protected String host;
    protected String user;
    protected String password;
    protected String database;
    protected int port = 1106;
    private SQLServerDataSource ds;

    public DbMSSQLConnectionProvider() {
    }

    public void connect() {
        ds = new SQLServerDataSource();
        ds.setServerName(getHost());
        ds.setDatabaseName(getDatabase());
        ds.setUser(getUser());
        ds.setPassword(getPassword());
        ds.setPortNumber(getPort());
    }

    @Override
    public Connection getConnection() {
        try {
            return ds.getConnection();
        } catch (SQLException e) {
            return null;
        }
    }

    public SQLServerDataSource getDataSource() {
        return ds;
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
