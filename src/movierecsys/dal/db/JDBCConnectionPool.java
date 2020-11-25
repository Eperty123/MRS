package movierecsys.dal.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import movierecsys.dal.ObjectPool;

public class JDBCConnectionPool extends ObjectPool<Connection> {

    private static JDBCConnectionPool INSTANCE;

    // 0 - MS SQL
    // 1 - MySQL
    private static int connectionType = 1;
    private final DbMSSQLConnectionProvider connectionMSSQLProvider;
    private final DbMysqlConnectionProvider connectionMySQLProvider;

    public synchronized static JDBCConnectionPool getInstance() throws IOException //I make the JDBC Connection Pool a Singleton.
    {
        if (INSTANCE == null)
            INSTANCE = new JDBCConnectionPool();
        return INSTANCE;
    }

    private JDBCConnectionPool() throws IOException {
        connectionMSSQLProvider = new DbMSSQLConnectionProvider();
        connectionMySQLProvider = new DbMysqlConnectionProvider();
    }

    @Override
    protected Connection create() {
        switch (connectionType) {
            case 0:
                return connectionMSSQLProvider.getConnection();

            case 1:
                return connectionMySQLProvider.getConnection();

            default:
                return null;
        }
    }

    @Override
    public boolean validate(Connection con) {
        try {
            return (!con.isClosed());
        } catch (SQLException e) {
            e.printStackTrace();
            return (false);
        }
    }

    @Override
    public void expire(Connection con) {
        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
