package me.x.impls.services;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.*;
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
                     "insert into prerequisite_list (type) values ('ATOM');"
             );
             PreparedStatement query = connection.prepareStatement(
                     "select count(*) from prerequisite_list;"
             );
             PreparedStatement second_stmt = connection.prepareStatement(
                     "insert into AtomPrerequisites (listId, courseId) values (?,?);"
             );
        ) {
            first_stmt.execute();
            ResultSet result = query.executeQuery();
            result.next();
            int list_id = result.getInt(1);
            second_stmt.setInt(1,list_id);
            second_stmt.setString(2, atomPrerequisite.courseID);
            return list_id;
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }

    public int add_And_Or_Prerequisite(ArrayList<Integer> terms, boolean isAndPrerequisite){
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_stmt = connection.prepareStatement(
                     "insert into prerequisite_list (type) values (?);"
             );
             PreparedStatement query = connection.prepareStatement(
                     "select count(*) from prerequisite_list;"
             );
             PreparedStatement second_stmt = connection.prepareStatement(
                     "insert into ? (listId, terms) values (?,?);"
             );
        ) {
            String table;
            if (isAndPrerequisite){
                first_stmt.setString(1,"AND");
                table = "AndPrerequisites";
            }else {
                first_stmt.setString(1,"OR");
                table = "OrPrerequisites";
            }

            first_stmt.execute();
            ResultSet result = query.executeQuery();
            result.next();
            int listId = result.getInt(1);
            second_stmt.setString(1,table);
            second_stmt.setInt(2,listId
            );
            Integer[] term_ids = terms.toArray(new Integer[0]);
            Array array = connection.createArrayOf("int", term_ids);
            second_stmt.setArray(3,array);
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
            return add_And_Or_Prerequisite(term_ids,true);
        }
        else {
            ArrayList<Integer> term_ids = new ArrayList<>();
            for (Prerequisite term :
                    ((OrPrerequisite)prerequisite).terms) {
                term_ids.add(handlePrerequisite(term));
            }
            return add_And_Or_Prerequisite(term_ids,false);
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
                     "insert into Courses (courseId, courseName, credit, classHour, grading, root_Prerequisite) values (?,?,?,?,?,?);")) {
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
                     "insert into courseSections (courseId, semesterId, sectionName, totalCapacity, leftCapacity) values (?,?,?,?,?);")) {
            stmt.setString(1, courseId);
            stmt.setInt(2,semesterId);
            stmt.setString(3,sectionName);
            stmt.setInt(4,totalCapacity);
            stmt.setInt(5,totalCapacity);
            return stmt.executeUpdate();
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
                     "insert into courseSectionClasses (sectionId, instructorId, dayOfWeek, weekList, classStart, classEnd, location) values (?,?,?,?,?,?,?);")) {
            stmt.setInt(1,sectionId);
            stmt.setInt(2,instructorId);
            stmt.setString(3, dayOfWeek.name());
            Short[] weeks = weekList.toArray(new Short[0]);
            Array array = connection.createArrayOf("int",weeks);
            stmt.setArray(4, array);
            stmt.setInt(5,classStart);
            stmt.setInt(6,classEnd);
            stmt.setString(7,location);
            return stmt.executeUpdate();
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
                     "delete from Courses where (courseId) = (?);");
             PreparedStatement query = connection.prepareStatement(
                     "select id from courseSections where courseId = (?) "
             )
        ) {
            stmt.setString(1, courseId);
            query.setString(1,courseId);
            stmt.execute();
            ResultSet resultSet = query.executeQuery();
            while (resultSet.next()){
                int sectionId = resultSet.getInt(1);
                removeCourseSection(sectionId);
            }
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
                     "delete from courseSections where (id) = (?);");
             PreparedStatement query = connection.prepareStatement(
                     "select id from courseSectionClasses where sectionId = (?)"
             )
        ) {
            stmt.setInt(1, sectionId);
            query.setInt(1,sectionId);
            stmt.execute();
            ResultSet resultSet = query.executeQuery();
            while (resultSet.next()){
                int classId = resultSet.getInt(1);
                removeCourseSectionClass(classId);
            }
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
                     "delete from courseSectionClasses where (id) = (?);")) {
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
             PreparedStatement stmt = connection.prepareStatement("select * from Courses;")) {
            ResultSet resultSet = stmt.executeQuery();
            while(resultSet.next()) {
                Course course = new Course();   //first column is the auto inc id
                course.id = resultSet.getString(2);
                course.name = resultSet.getString(3);
                course.credit = resultSet.getInt(4);
                course.classHour = resultSet.getInt(5);
                String grading = resultSet.getString(6);
                if (grading.equals("PASS_OR_FAIL")){
                    course.grading = PASS_OR_FAIL;
                }
                else {
                    course.grading = HUNDRED_MARK_SCORE;
                }
                courses.add(course);
            }
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
        return courses;
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
             PreparedStatement stmt = connection.prepareStatement("select * from courseSections where courseId = (?) and semesterId = (?);")) {
            stmt.setString(1,courseId);
            stmt.setInt(2,semesterId);
            ResultSet resultSet = stmt.executeQuery();
            boolean empty = true;
            while (resultSet.next()){
                empty = false;
                CourseSection courseSection= new CourseSection();
                courseSection.id = resultSet.getInt(1);
                courseSection.name = resultSet.getString(4);
                courseSection.totalCapacity = resultSet.getInt(5);
                courseSection.leftCapacity = resultSet.getInt(6);
                sections.add(courseSection);
            }

            if (empty){
                throw new EntityNotFoundException();
            }
            else {
                return sections;
            }
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }



    public Course getCourseByCourseId(String courseId){
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement getCourse = connection.prepareStatement("select * from Courses where courseId = (?);")) {

            getCourse.setString(1, courseId);
            ResultSet result = getCourse.executeQuery();
            result.next();
            Course course = new Course();    //first column is the auto inc id
            course.id = result.getString(2);
            course.name = result.getString(3);
            course.credit = result.getInt(4);
            course.classHour = result.getInt(5);
            String grading = result.getString(6);
            if (grading.equals("PASS_OR_FAIL")){
                course.grading = PASS_OR_FAIL;
            }else {
                course.grading = HUNDRED_MARK_SCORE;
            }
            return course;

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
             PreparedStatement first_query = connection.prepareStatement("select courseId from courseSections where id = (?);");
             PreparedStatement second_query = connection.prepareStatement("select * from Courses where courseId = (?);")) {
            first_query.setInt(1, sectionId);
            ResultSet resultSet = first_query.executeQuery();
            String courseId = null;
            boolean empty = true;
            while (resultSet.next()){
                empty = false;
                courseId = resultSet.getString(1);
            };

            if (empty){
                throw new EntityNotFoundException();
            }
            else {
                second_query.setString(1, courseId);
                second_query.execute();
                ResultSet result = second_query.getResultSet();
                result.next();
                Course course = new Course();    //first column is the auto inc id
                course.id = result.getString(2);
                course.name = result.getString(3);
                course.credit = result.getInt(4);
                course.classHour = result.getInt(5);
                String grading = result.getString(6);
                if (grading.equals("PASS_OR_FAIL")){
                    course.grading = PASS_OR_FAIL;
                }else {
                    course.grading = HUNDRED_MARK_SCORE;
                }
                return course;
            }
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
             PreparedStatement first_query = connection.prepareStatement("select * from courseSectionClasses where sectionId = (?);")
        ) {
            first_query.setInt(1, sectionId);
            ResultSet resultSet = first_query.executeQuery();
            boolean empty = true;
            while (resultSet.next()){
                empty = false;
                CourseSectionClass courseSectionClass = new CourseSectionClass();
                int instructorId = resultSet.getInt(3);
                MyUserService myUserService = new MyUserService();
                Instructor instructor = (Instructor)myUserService.getUser(instructorId);
                courseSectionClass.id = resultSet.getInt(1);
                courseSectionClass.instructor = instructor;
                String dayOfWeek = resultSet.getString(4);
                switch (dayOfWeek){
                    case "MONDAY":
                        courseSectionClass.dayOfWeek = DayOfWeek.MONDAY;
                        break;
                    case "TUESDAY":
                        courseSectionClass.dayOfWeek = DayOfWeek.TUESDAY;
                        break;
                    case "WEDNESDAY":
                        courseSectionClass.dayOfWeek = DayOfWeek.WEDNESDAY;
                        break;
                    case "THURSDAY":
                        courseSectionClass.dayOfWeek = DayOfWeek.THURSDAY;
                        break;
                    case "FRIDAY":
                        courseSectionClass.dayOfWeek = DayOfWeek.FRIDAY;
                        break;
                    case "SATURDAY":
                        courseSectionClass.dayOfWeek = DayOfWeek.SATURDAY;
                        break;
                    case "SUNDAY":
                        courseSectionClass.dayOfWeek = DayOfWeek.SUNDAY;
                        break;
                }
                Array array = resultSet.getArray(5);
                Set<Short> weekList = new HashSet<>();
                ResultSet res = array.getResultSet();
                while (res.next()){
                    weekList.add(res.getShort(2));  //second column stores value
                }
                courseSectionClass.weekList = weekList;
                courseSectionClass.classBegin = resultSet.getShort(6);
                courseSectionClass.classEnd = resultSet.getShort(7);
                courseSectionClass.location = resultSet.getString(8);

                classes.add(courseSectionClass);
            }

            if (empty){
                throw new EntityNotFoundException();
            }
            else {
                return classes;
            }
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
             PreparedStatement first_query = connection.prepareStatement("select sectionId from courseSectionClasses where id = (?);");
             PreparedStatement second_query = connection.prepareStatement("select * from courseSections where id = (?);")
        ) {
            first_query.setInt(1,classId);
            ResultSet resultSet = first_query.executeQuery();
            boolean empty = true;
            int sectionId = 0;
            while (resultSet.next()){
                empty = false;
                sectionId = resultSet.getInt(2);
            }
            if (empty){
                throw new EntityNotFoundException();
            }
            else {
                CourseSection courseSection = new CourseSection();
                second_query.setInt(1,sectionId);
                ResultSet result = second_query.executeQuery();
                result.next();
                courseSection.id = result.getInt(1);
                courseSection.name = result.getString(4);
                courseSection.totalCapacity = result.getInt(5);
                courseSection.leftCapacity = result.getInt(6);
                return courseSection;
            }
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
                     "select studentId from students_sections where semesterId = (?) and courseId = (?) ");
        ) {
            getStudentId.setString(1,courseId);
            getStudentId.setInt(2,semesterId);
            ResultSet studentsSet = getStudentId.executeQuery();

            MyStudentService studentService = new MyStudentService();
            while (studentsSet.next()){
                int studentId = studentsSet.getInt(1);
                students.add(studentService.getStudentById(studentId));
            }
            return students;
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }
}
