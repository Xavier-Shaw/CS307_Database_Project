package me.x.impls.services;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.*;
import cn.edu.sustech.cs307.dto.grade.Grade;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.service.StudentService;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.DayOfWeek;
import java.util.List;
import java.util.Map;

public class MyStudentService implements StudentService {
    private List<Student> studentList;


    @Override
    public void addStudent(int userId, int majorId, String firstName, String lastName, Date enrolledDate) {
        Student now = new Student();
        now.id = userId;

        //
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("");
             PreparedStatement second_query = connection.prepareStatement("")
        ) {

        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }

    @Override
    public List<CourseSearchEntry> searchCourse(int studentId, int semesterId, @Nullable String searchCid, @Nullable String searchName, @Nullable String searchInstructor, @Nullable DayOfWeek searchDayOfWeek, @Nullable Short searchClassTime, @Nullable List<String> searchClassLocations, CourseType searchCourseType, boolean ignoreFull, boolean ignoreConflict, boolean ignorePassed, boolean ignoreMissingPrerequisites, int pageSize, int pageIndex) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("");
             PreparedStatement second_query = connection.prepareStatement("")
        ) {

        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }

    @Override
    public EnrollResult enrollCourse(int studentId, int sectionId) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("");
             PreparedStatement second_query = connection.prepareStatement("")
        ) {

        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }

    @Override
    public void dropCourse(int studentId, int sectionId) throws IllegalStateException {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("");
             PreparedStatement second_query = connection.prepareStatement("")
        ) {

        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }

    @Override
    public void addEnrolledCourseWithGrade(int studentId, int sectionId, @Nullable Grade grade) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("");
             PreparedStatement second_query = connection.prepareStatement("")
        ) {

        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }

    @Override
    public void setEnrolledCourseGrade(int studentId, int sectionId, Grade grade) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("");
             PreparedStatement second_query = connection.prepareStatement("")
        ) {

        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }

    @Override
    public Map<Course, Grade> getEnrolledCoursesAndGrades(int studentId, @Nullable Integer semesterId) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("");
             PreparedStatement second_query = connection.prepareStatement("")
        ) {

        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }

    @Override
    public CourseTable getCourseTable(int studentId, Date date) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("");
             PreparedStatement second_query = connection.prepareStatement("")
        ) {

        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }

    @Override
    public boolean passedPrerequisitesForCourse(int studentId, String courseId) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("");
             PreparedStatement second_query = connection.prepareStatement("")
        ) {

        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }

    @Override
    public Major getStudentMajor(int studentId) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("");
             PreparedStatement second_query = connection.prepareStatement("")
        ) {

        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }
}
