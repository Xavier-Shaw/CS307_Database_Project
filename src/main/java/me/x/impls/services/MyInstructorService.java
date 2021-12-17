package me.x.impls.services;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.CourseSection;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.service.InstructorService;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MyInstructorService implements InstructorService {
    @Override
    public void addInstructor(int userId, String firstName, String lastName) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("insert into Instructors (userId, firstName, lastName) values (?,?,?)");
        ) {
            // Add a user account for this instructor as well
            MyUserService myUserService = new MyUserService();
            myUserService.addUser(userId, firstName, lastName);

            first_query.setInt(1,userId);
            first_query.setString(2,firstName);
            first_query.setString(3,lastName);
            first_query.execute();

        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }


    public CourseSection getCourseSectionByIdAndSemester(int sectionId, int semesterId){
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement stmt = connection.prepareStatement("select * from courseSections where sectionId = (?) and semesterId = (?);")) {
            stmt.setInt(1,sectionId);
            stmt.setInt(2,semesterId);
            ResultSet resultSet = stmt.executeQuery();

            if (resultSet.next()) {
                CourseSection courseSection = new CourseSection();
                courseSection.id = resultSet.getInt(1);
                courseSection.name = resultSet.getString(4);
                courseSection.totalCapacity = resultSet.getInt(5);
                courseSection.leftCapacity = resultSet.getInt(6);
                return courseSection;
            }

            return null;
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }


    @Override
    public List<CourseSection> getInstructedCourseSections(int instructorId, int semesterId) {
        ArrayList<CourseSection> courseSections = new ArrayList<>();
        //courseId, semesterId, sectionName, totalCapacity, leftCapacity
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("select sectionId from courseSectionClasses where instructorId = ? ;");
        ) {
            first_query.setInt(1,instructorId);
            ResultSet resultSet = first_query.executeQuery();
            while (resultSet.next()){
                int sectionId = resultSet.getInt(1);
                CourseSection courseSection = getCourseSectionByIdAndSemester(sectionId,semesterId);
                if (courseSection != null){
                    courseSections.add(courseSection);
                }
            }

            return courseSections;
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }
}
