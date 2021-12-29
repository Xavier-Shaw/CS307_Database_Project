package me.x.impls.services;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.*;
import cn.edu.sustech.cs307.dto.grade.Grade;
import cn.edu.sustech.cs307.dto.prerequisite.AndPrerequisite;
import cn.edu.sustech.cs307.dto.prerequisite.CoursePrerequisite;
import cn.edu.sustech.cs307.dto.prerequisite.OrPrerequisite;
import cn.edu.sustech.cs307.dto.prerequisite.Prerequisite;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.service.CourseService;
import cn.edu.sustech.cs307.service.UserService;

import javax.annotation.Nullable;
import java.sql.*;
import java.time.DayOfWeek;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static cn.edu.sustech.cs307.dto.Course.CourseGrading.HUNDRED_MARK_SCORE;
import static cn.edu.sustech.cs307.dto.Course.CourseGrading.PASS_OR_FAIL;

public class MyCourseService implements CourseService {

    public int addAtomPrerequisite(CoursePrerequisite atomPrerequisite){
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_stmt = connection.prepareStatement(
                     "insert into prerequisite_list (type) values ('Atom');"
             );
             PreparedStatement query = connection.prepareStatement(
                     "select count(*) from prerequisite_list;"
             );
             PreparedStatement second_stmt = connection.prepareStatement(
                     "insert into \"AtomPrerequisites\" (\"listId\", \"courseId\") values (?,?);"
             );
        ) {
            first_stmt.execute();
            ResultSet result = query.executeQuery();
            result.next();
            int list_id = result.getInt(1);
            second_stmt.setInt(1,list_id);
            second_stmt.setString(2, atomPrerequisite.courseID);
            second_stmt.execute();
            return list_id;
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }


    public int add_Or_Prerequisite(ArrayList<Integer> terms){
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_stmt = connection.prepareStatement(
                     "insert into prerequisite_list (type) values ('Or');"
             );
             PreparedStatement query = connection.prepareStatement(
                     "select count(*) from prerequisite_list;"
             );
             PreparedStatement second_stmt = connection.prepareStatement(
                     "insert into \"OrPrerequisites\" (\"listId\", terms) values (?,?);"
             );
        ) {

            first_stmt.execute();
            ResultSet result = query.executeQuery();
            result.next();
            int listId = result.getInt(1);

            second_stmt.setInt(1,listId);
            Integer[] term_ids = terms.toArray(new Integer[0]);
            Array array = connection.createArrayOf("int", term_ids);
            second_stmt.setArray(2,array);
            second_stmt.execute();

            return listId;
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }

    public int add_And_Prerequisite(ArrayList<Integer> terms){
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_stmt = connection.prepareStatement(
                     "insert into prerequisite_list (type) values ('And');"
             );
             PreparedStatement query = connection.prepareStatement(
                     "select count(*) from prerequisite_list;"
             );
             PreparedStatement second_stmt = connection.prepareStatement(
                     "insert into \"AndPrerequisites\" (\"listId\", terms) values (?,?);"
             );
        ) {
            first_stmt.execute();
            ResultSet result = query.executeQuery();
            result.next();
            int listId = result.getInt(1);

            second_stmt.setInt(1,listId);
            Integer[] term_ids = terms.toArray(new Integer[0]);
            Array array = connection.createArrayOf("int", term_ids);
            second_stmt.setArray(2,array);
            second_stmt.execute();

            return listId;
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }

    public int handlePrerequisite(Prerequisite prerequisite){
        if (prerequisite instanceof CoursePrerequisite){
            return addAtomPrerequisite((CoursePrerequisite) prerequisite);
        }
        else if (prerequisite instanceof AndPrerequisite){
            ArrayList<Integer> term_ids = new ArrayList<>();
            for (Prerequisite term:
                 ((AndPrerequisite) prerequisite).terms) {
                term_ids.add(handlePrerequisite(term));
            }
            return add_And_Prerequisite(term_ids);
        }
        else {
            ArrayList<Integer> term_ids = new ArrayList<>();
            for (Prerequisite term :
                    ((OrPrerequisite)prerequisite).terms) {
                term_ids.add(handlePrerequisite(term));
            }
            return add_Or_Prerequisite(term_ids);
        }
    }


    /**
     * Add one course according to following parameters.
     * If some of parameters are invalid, throw {@link cn.edu.sustech.cs307.exception.IntegrityViolationException}
     *
     * @param courseId represents the id of course. For example, CS307, CS309
     * @param courseName the name of course
     * @param credit the credit of course
     * @param classHour The total teaching hour that the course spends.
     * @param grading the grading type of course
     * @param prerequisite The root of a {@link cn.edu.sustech.cs307.dto.prerequisite.Prerequisite} expression tree.
     */
    @Override
    public void addCourse(String courseId, String courseName, int credit, int classHour, Course.CourseGrading grading, @Nullable Prerequisite prerequisite) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement stmt = connection.prepareStatement(
                     "insert into \"Courses\" (\"courseId\", \"courseName\", credit, \"classHour\", grading, root_prerequisite) values (?,?,?,?,?,?);")) {
            stmt.setString(1, courseId);
            stmt.setString(2,courseName);
            stmt.setInt(3,credit);
            stmt.setInt(4,classHour);
            stmt.setString(5,grading.name());
            if (prerequisite == null){
                stmt.setInt(6,0);
            }else {
                stmt.setInt(6, handlePrerequisite(prerequisite));
            }
            stmt.execute();
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }

    /**
     * Add one course section according to following parameters:
     * If some of parameters are invalid, throw {@link cn.edu.sustech.cs307.exception.IntegrityViolationException}
     *
     * @param courseId represents the id of course. For example, CS307, CS309
     * @param semesterId the id of semester
     * @param sectionName the name of section {@link cn.edu.sustech.cs307.dto.CourseSection}
     * @param totalCapacity the total capacity of section
     * @return the CourseSection id of new inserted line, if adding process is successful.
     */
    @Override
    public int addCourseSection(String courseId, int semesterId, String sectionName, int totalCapacity) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement stmt = connection.prepareStatement(
                     "insert into \"courseSections\" (\"courseId\", \"semesterId\", \"sectionName\", \"totalCapacity\", \"leftCapacity\") values (?,?,?,?,?);");
             PreparedStatement query = connection.prepareStatement(
                     "select id from \"courseSections\" where (\"courseId\", \"semesterId\", \"sectionName\", \"totalCapacity\", \"leftCapacity\") = (?,?,?,?,?)"
             )
        ) {
            stmt.setString(1, courseId);
            stmt.setInt(2,semesterId);
            stmt.setString(3,sectionName);
            stmt.setInt(4,totalCapacity);
            stmt.setInt(5,totalCapacity);
            stmt.execute();

            query.setString(1, courseId);
            query.setInt(2,semesterId);
            query.setString(3,sectionName);
            query.setInt(4,totalCapacity);
            query.setInt(5,totalCapacity);

            ResultSet resultSet = query.executeQuery();
            resultSet.next();
            return resultSet.getInt(1);
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }

    /**
     * Add one course section class according to following parameters:
     * If some of parameters are invalid, throw {@link cn.edu.sustech.cs307.exception.IntegrityViolationException}
     *
     * @param sectionId
     * @param instructorId
     * @param dayOfWeek
     * @param weekList
     * @param classStart
     * @param classEnd
     * @param location
     * @return the CourseSectionClass id of new inserted line.
     */
    @Override
    public int addCourseSectionClass(int sectionId, int instructorId, DayOfWeek dayOfWeek, Set<Short> weekList, short classStart, short classEnd, String location) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement stmt = connection.prepareStatement(
                     "insert into \"courseSectionClasses\" (\"sectionId\", \"instructorId\", \"dayOfWeek\", \"weekList\", \"classStart\", \"classEnd\", location) " +
                             "values (?,?,?,?,?,?,?);");
             PreparedStatement query = connection.prepareStatement(
                     "select id from \"courseSectionClasses\" where (\"sectionId\", \"instructorId\", \"dayOfWeek\", \"weekList\", \"classStart\", \"classEnd\", location) " +
                             "= (?,?,?,?,?,?,?);"
             )
             ) {
            stmt.setInt(1,sectionId);
            stmt.setInt(2,instructorId);
            stmt.setString(3, dayOfWeek.name());
            Short[] weeks = weekList.toArray(new Short[0]);
            Array array = connection.createArrayOf("int",weeks);
            stmt.setArray(4, array);
            stmt.setShort(5,classStart);
            stmt.setShort(6,classEnd);
            stmt.setString(7,location);
            stmt.execute();

            query.setInt(1,sectionId);
            query.setInt(2,instructorId);
            query.setString(3, dayOfWeek.toString());
            query.setArray(4, array);
            query.setShort(5,classStart);
            query.setShort(6,classEnd);
            query.setString(7,location);

            ResultSet resultSet = query.executeQuery();
            resultSet.next();
            return resultSet.getInt(1);
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }



    /**
     * To remove an entity from the system, related entities dependent on this entity (usually rows referencing the row to remove through foreign keys in a relational database)
     * shall be removed together.
     * More specifically, remove all related courseSection, all related CourseSectionClass and all related select course records
     * when a course has been removed
     * @param courseId
     */
    @Override
    public void removeCourse(String courseId) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement stmt = connection.prepareStatement(
                     "delete from \"Courses\" where (\"courseId\") = (?);");
        ) {
            stmt.setString(1, courseId);
            stmt.execute();

        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }


    /**
     *  To remove an entity from the system, related entities dependent on this entity
     *  (usually rows referencing the row to remove through foreign keys in a relational database)
     *  shall be removed together.
     *   More specifically, remove all related CourseSectionClass and all related select course records
     *   when a courseSection has been removed
     * @param sectionId
     */
    @Override
    public void removeCourseSection(int sectionId) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement stmt = connection.prepareStatement(
                     "delete from \"courseSections\" where (id) = (?);");
        ) {
            stmt.setInt(1, sectionId);
            stmt.execute();

        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }


    /**
     *  To remove an entity from the system, related entities dependent on this entity (usually rows referencing the row to remove through foreign keys in a relational database)
     *  shall be removed together.
     *  More specifically, only remove course section class
     * @param classId
     */
    @Override
    public void removeCourseSectionClass(int classId) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement stmt = connection.prepareStatement(
                     "delete from \"courseSectionClasses\" where (id) = (?);")) {
            stmt.setInt(1, classId);
            stmt.execute();
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }


    @Override
    public List<Course> getAllCourses() {
        ArrayList<Course> courses = new ArrayList<>();
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement stmt = connection.prepareStatement("select * from \"Courses\";")) {
            ResultSet resultSet = stmt.executeQuery();
            while(resultSet.next()) {
                Course course = new Course();   //first column is the auto inc id
                course.id = resultSet.getString(2);
                course.name = resultSet.getString(3);
                course.credit = resultSet.getInt(4);
                course.classHour = resultSet.getInt(5);
                course.grading = Course.CourseGrading.valueOf(resultSet.getString(6));
                courses.add(course);
            }
            return courses;
        } catch (SQLException e) {
            return courses;
        }
    }



    /**
     * Return all satisfied CourseSections.
     * We will compare the all other fields in CourseSection besides the id.
     * @param courseId if the key is non-existent, please throw an EntityNotFoundException.
     * @param semesterId if the key is non-existent, please throw an EntityNotFoundException.
     * @return
     *
     * id, courseId, semesterId, sectionName, totalCapacity, leftCapacity
     */

    @Override
    public List<CourseSection> getCourseSectionsInSemester(String courseId, int semesterId) {
        ArrayList<CourseSection> sections = new ArrayList<>();
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement stmt = connection.prepareStatement(
                     "select * from \"courseSections\" where \"courseId\" = (?) and \"semesterId\" = (?);")) {

            stmt.setString(1,courseId);
            stmt.setInt(2,semesterId);
            ResultSet resultSet = stmt.executeQuery();

            while (resultSet.next()){
                CourseSection courseSection= new CourseSection();
                courseSection.id = resultSet.getInt(1);
                courseSection.name = resultSet.getString(4);
                courseSection.totalCapacity = resultSet.getInt(5);
                courseSection.leftCapacity = resultSet.getInt(6);
                sections.add(courseSection);
            }


            return sections;

        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }



    /**
     * If there is no Course about specific id, throw EntityNotFoundException.
     * @param sectionId if the key is non-existent, please throw an EntityNotFoundException.
     * @return
     */
    @Override
    public Course getCourseBySection(int sectionId) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("select \"courseId\" from \"courseSections\" where id = (?);");
             PreparedStatement second_query = connection.prepareStatement("select \"courseName\", credit, \"classHour\", grading from \"Courses\" where \"courseId\" = (?);")
        ) {
            first_query.setInt(1, sectionId);
            ResultSet resultSet = first_query.executeQuery();

            resultSet.next();
            String courseId = resultSet.getString(1);

            second_query.setString(1, courseId);
            ResultSet result = second_query.executeQuery();
            result.next();
            Course course = new Course();    //first column is the auto inc id
            course.id = courseId;
            course.name = result.getString(1);
            course.credit = result.getInt(2);
            course.classHour = result.getInt(3);
            course.grading = Course.CourseGrading.valueOf(result.getString(4));

            return course;
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }


    /**
     *
     * @param sectionId the id of {@code CourseSection}
     *                  if the key is non-existent, please throw an EntityNotFoundException.
     * @return
     * sectionId, instructorId, dayOfWeek, weekList, classStart, classEnd, location
     */
    @Override
    public List<CourseSectionClass> getCourseSectionClasses(int sectionId) {
        ArrayList<CourseSectionClass> classes = new ArrayList<>();
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement(
                     "select * from \"courseSectionClasses\" where \"sectionId\" = (?);")
        ) {
            first_query.setInt(1, sectionId);
            ResultSet resultSet = first_query.executeQuery();

            while (resultSet.next()){
                CourseSectionClass courseSectionClass = new CourseSectionClass();
                int instructorId = resultSet.getInt(3);
                MyUserService myUserService = new MyUserService();
                Instructor instructor = (Instructor)myUserService.getUser(instructorId);
                courseSectionClass.id = resultSet.getInt(1);
                courseSectionClass.instructor = instructor;
                courseSectionClass.dayOfWeek = DayOfWeek.valueOf(resultSet.getString(4));
                Array array = resultSet.getArray(5);

                ResultSet res = array.getResultSet();
                courseSectionClass.weekList = new HashSet<>();
                while (res.next()){
                    courseSectionClass.weekList.add(res.getShort(2));
                }

                courseSectionClass.classBegin = resultSet.getShort(6);
                courseSectionClass.classEnd = resultSet.getShort(7);
                courseSectionClass.location = resultSet.getString(8);

                classes.add(courseSectionClass);
            }

            return classes;

        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }


    /**
     *
     * @param classId if the key is non-existent, please throw an EntityNotFoundException.
     * @return
     * id, courseId, semesterId, sectionName, totalCapacity, leftCapacity
     */
    @Override
    public CourseSection getCourseSectionByClass(int classId) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement(
                     "select \"sectionId\" from \"courseSectionClasses\" where id = (?);");
             PreparedStatement second_query = connection.prepareStatement(
                     "select id, \"sectionName\", \"totalCapacity\", \"leftCapacity\" from \"courseSections\" where id = (?);")
        ) {
            first_query.setInt(1,classId);
            ResultSet resultSet = first_query.executeQuery();

            resultSet.next();
            int sectionId = resultSet.getInt(1);


            CourseSection courseSection = new CourseSection();
            second_query.setInt(1,sectionId);
            ResultSet result = second_query.executeQuery();
            result.next();
            courseSection.id = result.getInt(1);
            courseSection.name = result.getString(2);
            courseSection.totalCapacity = result.getInt(3);
            courseSection.leftCapacity = result.getInt(4);
            return courseSection;

        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }


    /**
     *
     * @param courseId  if the key is non-existent, please throw an EntityNotFoundException.
     * @param semesterId if the key is non-existent, please throw an EntityNotFoundException.
     * @return
     */
    @Override
    public List<Student> getEnrolledStudentsInSemester(String courseId, int semesterId) {
        ArrayList<Student> students = new ArrayList<>();
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement getStudentId = connection.prepareStatement(
                     "select get_student_in_semester(?,?)");
        ) {
            getStudentId.setString(1,courseId);
            getStudentId.setInt(2,semesterId);
            ResultSet resultSet = getStudentId.executeQuery();

            while (resultSet.next()){
                Student student = new Student();
                student.id = resultSet.getInt(1);
                String firstName = resultSet.getString(2);
                String lastName = resultSet.getString(3);
                byte judge = (byte) firstName.charAt(0);
                String fullName;
                if ((judge >= 65 && judge <= 90) || (judge >= 97 && judge <= 122)) {
                    fullName = firstName + " " + lastName;
                } else {
                    fullName = firstName + lastName;
                }
                student.fullName = fullName;
                student.enrolledDate = resultSet.getDate(4);

                Major major = new Major();
                major.id = resultSet.getInt(5);
                major.name = resultSet.getString(6);
                Department department = new Department();
                department.id = resultSet.getInt(7);
                department.name = resultSet.getString(8);
                major.department = department;

                student.major = major;

                students.add(student);
            }
            return students;
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }
}
