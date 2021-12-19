package me.x.impls.services;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.Semester;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.SemesterService;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MySemesterService implements SemesterService {

    /**
     * Add one semester according to following parameters:
     * If some of parameters are invalid, throw {@link cn.edu.sustech.cs307.exception.IntegrityViolationException}
     * @param name
     * @param begin
     * @param end
     * @return the Semester id of new inserted line, if adding process is successful.
     */
    @Override
    public int addSemester(String name, Date begin, Date end) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("insert into Semesters (name, begin_date, end_date) values (?,?,?) ;");
        ) {
            if (begin.after(end)){
                throw new IntegrityViolationException();
            }

            first_query.setString(1,name);
            first_query.setDate(2,begin);
            first_query.setDate(3,end);
            return first_query.executeUpdate();

        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }

    /**
     *To remove an entity from the system, related entities dependent on this entity
     *  (usually rows referencing the row to remove through foreign keys in a relational database) shall be removed together.
     *
     * More specifically, when remove a semester, the related select course record should be removed accordingly.
     * @param semesterId
     */
    @Override
    public void removeSemester(int semesterId) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("delete from Semesters where id = (?)");
        ) {
            first_query.setInt(1,semesterId);
            first_query.execute();
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }

    @Override
    public List<Semester> getAllSemesters() {
        ArrayList<Semester> semesters = new ArrayList<>();
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("select * from Semesters");
        ) {
            ResultSet resultSet = first_query.executeQuery();
            while (resultSet.next()){
                Semester semester = new Semester();
                semester.id = resultSet.getInt(1);
                semester.name = resultSet.getString(2);
                semester.begin = resultSet.getDate(3);
                semester.end = resultSet.getDate(4);
                semesters.add(semester);
            }
            return semesters;
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }

    @Override
    public Semester getSemester(int semesterId) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("select * from Semesters where id = (?)");
        ) {
            first_query.setInt(1,semesterId);
            ResultSet resultSet = first_query.executeQuery();
            if (resultSet.next()){
                Semester semester = new Semester();
                semester.id = semesterId;
                semester.name = resultSet.getString(2);
                semester.begin = resultSet.getDate(3);
                semester.end = resultSet.getDate(4);
                return semester;
            }else {
                throw new EntityNotFoundException();
            }
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }
}
