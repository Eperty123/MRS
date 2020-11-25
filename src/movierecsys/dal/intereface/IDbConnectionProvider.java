package movierecsys.dal.intereface;

import java.sql.Connection;

public interface IDbConnectionProvider {

    Connection getConnection();

    String getDatabase();

    String getHost();

    String getUser();

    String getPassword();

    int getPort();

    void connect();

    void setDatabase(String database);

    void setHost(String host);

    void setUser(String user);

    void setPassword(String password);

    void setPort(int port);
}
