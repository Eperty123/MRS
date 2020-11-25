/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package movierecsys.dal;

import movierecsys.dal.db.DbMysqlConnectionProvider;
import movierecsys.dal.file.RatingDAO;
import movierecsys.dal.file.MovieDAO;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import movierecsys.be.Movie;
import movierecsys.be.Rating;
import movierecsys.be.User;
import movierecsys.dal.db.DbMSSQLConnectionProvider;
import movierecsys.dal.exception.MrsDalException;
import movierecsys.dal.file.UserDAO;

/**
 * @author pgn
 */
public class FileReaderTester {

    /**
     * Example method. This is the code I used to create the users.txt files.
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException, MrsDalException, SQLException {

        //     mitigateMovies();
        //     mitigateUsers();
        //mitigateRatings();
        mitigateMySQLMovies();
    }

    public static void mysqlTest() throws MrsDalException {
        DbMysqlConnectionProvider ds = new DbMysqlConnectionProvider();
        List<User> users = new UserDAO().getAllUsers();

        try (Connection con = ds.getConnection()) {
            int counter = 0;
            for (User user : users) {
                String sql = "INSERT INTO user (id,name) VALUES(?,?);";


                PreparedStatement statement = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                statement.setInt(1, user.getId());
                statement.setString(2, user.getName());
                statement.execute();

                counter++;
                System.out.println("Added " + counter);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void mitigateUsers() throws IOException, MrsDalException {
        DbMSSQLConnectionProvider ds = new DbMSSQLConnectionProvider();

        List<User> users = new UserDAO().getAllUsers();

        try (Connection con = ds.getConnection()) {
            Statement statement = con.createStatement();
            int counter = 0;
            for (User user : users) {
                String sql = "INSERT INTO [User] (id,name) VALUES("
                        + user.getId() + ",'"
                        + user.getName() + "');";
                statement.addBatch(sql);
                counter++;
                if (counter % 10000 == 0) {
                    statement.executeBatch();
                    System.out.println("Added " + counter + " of " + users.size() + " users.");
                }
            }
            statement.executeBatch();
            System.out.println("Added " + counter + " of " + users.size() + " users.");
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Untested batch insert. We'll see how it goes tomorrow.
     *
     * @throws IOException
     */
    public static void mitigateRatings() throws IOException {
        List<Rating> allRatings = new RatingDAO().getAllRatings();
        DbMSSQLConnectionProvider ds = new DbMSSQLConnectionProvider();
        try (Connection con = ds.getConnection()) {
            Statement st = con.createStatement();
            int counter = 0;
            for (Rating rating : allRatings) {
                String sql = "INSERT INTO rating (movie_id, user_id, score) VALUES ("
                        + rating.getMovie() + ","
                        + rating.getUser() + ","
                        + rating.getRating()
                        + ");";
                st.addBatch(sql);
                counter++;
                if (counter % 10000 == 0) {
                    st.executeBatch();
                    System.out.println("Inserted " + counter + " of " + allRatings.size() + " ratings.");
                }
            }
            st.executeBatch();
            System.out.println("Inserted " + counter + " of " + allRatings.size() + " ratings.");
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void mitigateMySQLMovies() throws IOException, MrsDalException {
        DbMysqlConnectionProvider ds = new DbMysqlConnectionProvider();
        MovieDAO mvDao = new MovieDAO();
        List<Movie> movies = mvDao.getAllMovies();

        try (Connection con = ds.getConnection()) {
            Statement statement = con.createStatement();
            int c = 0;
            for (Movie movie : movies) {
                String sql = "INSERT INTO Movie (id,year,title) VALUES("
                        + movie.getId() + ","
                        + movie.getYear() + ",'"
                        + movie.getTitle().replace("'", "") + "');";
                statement.addBatch(sql);
                c++;
                if (c % 1000 == 0) {
                    System.out.println("Inserted " + c + " of " + movies.size() + " movies.");
                    statement.executeBatch();
                }
            }
            statement.executeBatch();
            System.out.println("Inserted " + c + " of " + movies.size() + " movies.");
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            System.out.println(ex.getSQLState());
            ex.printStackTrace();
        } catch (NoClassDefFoundError noClassDefFoundError) {
            System.out.println(noClassDefFoundError.getMessage());
            System.out.println(noClassDefFoundError.getCause().getClass().getName());
            noClassDefFoundError.printStackTrace();
        }
    }

    public static void mitigateMovies() throws IOException, MrsDalException {
        DbMSSQLConnectionProvider ds = new DbMSSQLConnectionProvider();
        MovieDAO mvDao = new MovieDAO();
        List<Movie> movies = mvDao.getAllMovies();

        try (Connection con = ds.getConnection()) {
            Statement statement = con.createStatement();
            int c = 0;
            for (Movie movie : movies) {
                String sql = "INSERT INTO Movie (id,year,title) VALUES("
                        + movie.getId() + ","
                        + movie.getYear() + ",'"
                        + movie.getTitle().replace("'", "") + "');";
                statement.addBatch(sql);
                c++;
                if (c % 1000 == 0) {
                    System.out.println("Inserted " + c + " of " + movies.size() + " movies.");
                    statement.executeBatch();
                }
            }
            statement.executeBatch();
            System.out.println("Inserted " + c + " of " + movies.size() + " movies.");
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            System.out.println(ex.getSQLState());
            ex.printStackTrace();
        } catch (NoClassDefFoundError noClassDefFoundError) {
            System.out.println(noClassDefFoundError.getMessage());
            System.out.println(noClassDefFoundError.getCause().getClass().getName());
            noClassDefFoundError.printStackTrace();
        }
    }

    public static void createRafFriendlyRatingsFile() throws IOException {
        String target = "data/user_ratings";
        RatingDAO ratingDao = new RatingDAO();
        List<Rating> all = ratingDao.getAllRatings();

        try (RandomAccessFile raf = new RandomAccessFile(target, "rw")) {
            for (Rating rating : all) {
                raf.writeInt(rating.getMovie());
                raf.writeInt(rating.getUser());
                raf.writeInt(rating.getRating());
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

}
