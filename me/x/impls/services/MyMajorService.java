package me.x.impls.services;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.Major;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.service.MajorService;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MyMajorService implements MajorService {

    @Override
    public int addMajor(String name, int departmentId) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("insert into Majors (name, departmentId) values (?,?);");
        ) {
            first_query.setString(1,name);
            first_query.setInt(2,departmentId);
            return first_query.executeUpdate();
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }


    /**
     * To remove an entity from the system, related entities dependent on this entity
     * (usually rows referencing the row to remove through foreign keys in a relational database) shall be removed together.
     *
     * More specifically, when remove a major, the related students should be removed accordingly
     * @param majorId
     */
    @Override
    public void removeMajor(int majorId) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("delete from Majors where id = (?)");
             PreparedStatement second_query = connection.prepareStatement("")
        ) {
            first_query.setInt(1,majorId);
            first_query.execute();
            //TODO: delete the corresponding students
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }


    @Override
    public List<Major> getAllMajors() {
        ArrayList<Major> majors = new ArrayList<>();
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("select * from Majors");
        ) {
            ResultSet resultSet = first_query.executeQuery();
            while (resultSet.next()){
                Major major = new Major();
                major.id = resultSet.getInt(1);
                major.name = resultSet.getString(2);
                major.department = new MyDepartmentService().getDepartment(resultSet.getInt(3));
                majors.add(major);
            }
            return majors;
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }


    /**
     * If there is no Major about specific id, throw EntityNotFoundException.
     * @param majorId
     * @return
     */
    @Override
    public Major getMajor(int majorId) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("select * from Majors where id = ?");
        ) {
            first_query.setInt(1,majorId);
            ResultSet resultSet = first_query.executeQuery();

            if (resultSet.next()){
                Major major = new Major();
                major.id = majorId;
                major.name = resultSet.getString(2);
                major.department = new MyDepartmentService().getDepartment(resultSet.getInt(3));
                return major;
            }else {
                throw new EntityNotFoundException();
            }
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }

    /**
     * Binding a course id {@code courseId} to major id {@code majorId}, and the selection is compulsory.
     * @param majorId the id of major
     * @param courseId the course id
     */
    @Override
    public void addMajorCompulsoryCourse(int majorId, String courseId) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("select id from Courses where courseId = (?);");
             PreparedStatement second_query = connection.prepareStatement("insert into majorCourses (majorId, courseId, selection) values (?, ?, 'COMPULSORY')")
        ) {
            first_query.setString(1,courseId);
            ResultSet resultSet = first_query.executeQuery();
            resultSet.next();
            int id = resultSet.getInt(1);
            second_query.setInt(1,majorId);
            second_query.setInt(2,id);
            second_query.execute();
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }


    /**
     * Binding a course id{@code courseId} to major id {@code majorId}, and the selection is elective.
     * @param majorId the id of major
     * @param courseId the course id
     */
    @Override
    public void addMajorElectiveCourse(int majorId, String courseId) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("select id from Courses where courseId = (?);");
             PreparedStatement second_query = connection.prepareStatement("insert into majorCourses (majorId, courseId, selection) values (?, ?, 'ELECTIVE')")
        ) {
            first_query.setString(1,courseId);
            ResultSet resultSet = first_query.executeQuery();
            resultSet.next();
            int id = resultSet.getInt(1);
            second_query.setInt(1,majorId);
            second_query.setInt(2,id);
            second_query.execute();
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }
}
