/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package movierecsys.dal.db;

import movierecsys.dal.intereface.IMovieRepository;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import movierecsys.be.Movie;
import movierecsys.dal.exception.MrsDalException;

/**
 * @author pgn
 */
public class MovieDbDao implements IMovieRepository {

    private final DbConnectionHandler connectionPool;

    public MovieDbDao() {
        connectionPool = DbConnectionHandler.getInstance();
    }

    @Override
    public Movie createMovie(int releaseYear, String title) throws MrsDalException {
        String sql = "INSERT INTO movie (year,title) VALUES(?,?);";
        Connection con = connectionPool.getConnection(); // <<< Using the object pool here <<<
        try (PreparedStatement st = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            st.setInt(1, releaseYear);
            st.setString(2, title);
            st.executeUpdate();
            ResultSet rs = st.getGeneratedKeys();
            int id = 0;
            if (rs.next()) {
                id = rs.getInt(1);
            }
            Movie movie = new Movie(id, releaseYear, title);
            return movie;
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new MrsDalException("Could not create movie.", ex);
        }
    }

    @Override
    public void deleteMovie(Movie movie) throws MrsDalException {
        String sql = "DELETE FROM movie WHERE title = ?;";
        Connection con = connectionPool.getConnection(); // <<< Using the object pool here <<<
        try (PreparedStatement st = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            st.setString(1, movie.getTitle());
            st.executeUpdate();

        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new MrsDalException("Could not create movie.", ex);
        }
    }

    @Override
    public List<Movie> getAllMovies() throws MrsDalException {
        List<Movie> movies = new ArrayList<>();
        Connection con = connectionPool.getConnection();
        try (Statement statement = con.createStatement()) {
            ResultSet rs = statement.executeQuery("SELECT * FROM movie;");
            while (rs.next()) {
                int id = rs.getInt("id");
                int year = rs.getInt("year");
                String title = rs.getString("title");
                Movie movie = new Movie(id, year, title);
                movies.add(movie);
            }
            return movies;
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new MrsDalException("Could not get all movies from database", ex);
        }
    }

    @Override
    public Movie getMovie(int id) throws MrsDalException {
        String sql = "SELECT * FROM movie WHERE id = ?;";
        Connection con = connectionPool.getConnection(); // <<< Using the object pool here <<<
        try (PreparedStatement st = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            st.setInt(1, id);

            st.executeUpdate();
            var resultSet = st.getResultSet();
            Movie movie = new Movie();
            movie.setId(resultSet.getInt("id"));
            movie.setTitle(resultSet.getString("title"));
            movie.setYear(resultSet.getInt("year"));
            return movie;
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new MrsDalException("Could not get movie.", ex);
        }
    }

    @Override
    public void updateMovie(Movie movie) throws MrsDalException {
        String sql = "UPDATE movie SET title = ?, year = ? WHERE id = ?;";
        Connection con = connectionPool.getConnection(); // <<< Using the object pool here <<<
        try (PreparedStatement st = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            st.setString(1, movie.getTitle());
            st.setInt(2, movie.getYear());
            st.setInt(3, movie.getId());
            st.executeUpdate();

        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new MrsDalException("Could not update movie.", ex);
        }
    }

}
