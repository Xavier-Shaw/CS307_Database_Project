package me.x.impls.services;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.*;
import cn.edu.sustech.cs307.dto.grade.Grade;
import cn.edu.sustech.cs307.dto.grade.HundredMarkGrade;
import cn.edu.sustech.cs307.dto.grade.PassOrFailGrade;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.StudentService;

import javax.annotation.Nullable;
import java.sql.*;
import java.time.DayOfWeek;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MyStudentService implements StudentService {


    /**
     * Add one student according to following parameters.
     * If some of parameters are invalid, throw {@link cn.edu.sustech.cs307.exception.IntegrityViolationException}
     *
     * @param userId
     * @param majorId
     * @param firstName
     * @param lastName
     * @param enrolledDate
     */
    @Override
    public void addStudent(int userId, int majorId, String firstName, String lastName, Date enrolledDate) {
        //
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement(
                     "insert into students (userId, majorId, firstName, lastName, enrolledDate) values (?,?,?,?,?); ");
        ) {
            //Add a user account for the student
            MyUserService userService = new MyUserService();
            userService.addUser(userId,firstName,lastName);

            first_query.setInt(1,userId);
            first_query.setInt(2,majorId);
            first_query.setString(3,firstName);
            first_query.setString(4,lastName);
            first_query.setDate(5,enrolledDate);
            first_query.execute();

        } catch (SQLException e) {
            throw new IntegrityViolationException();
        }
    }


    /**
     * Search available courses (' sections) for the specified student in the semester with extra conditions.
     * The result should be first sorted by course ID, and then sorted by course full name (course.name[section.name]).
     * Ignore all course sections that have no sub-classes.
     * Note: All ignore* arguments are about whether or not the result should ignore such cases.
     * i.e. when ignoreFull is true, the result should filter out all sections that are full.
     *
     * @param studentId
     * @param semesterId
     * @param searchCid                  search course id. Rule: searchCid in course.id
     * @param searchName                 search course name. Rule: searchName in "course.name[section.name]"
     * @param searchInstructor           search instructor name.
     *                                   Rule: firstName + lastName begins with searchInstructor
     *                                   or firstName + ' ' + lastName begins with searchInstructor
     *                                   or firstName begins with searchInstructor
     *                                   or lastName begins with searchInstructor.
     * @param searchDayOfWeek            search day of week. Matches *any* class in the section in the search day of week.
     * @param searchClassTime            search class time. Matches *any* class in the section contains the search class time.
     * @param searchClassLocations       search class locations. Matches *any* class in the section contains *any* location from the search class locations.
     * @param searchCourseType           search course type. See {@link cn.edu.sustech.cs307.service.StudentService.CourseType}
     * @param ignoreFull                 whether or not to ignore full course sections.
     * @param ignoreConflict             whether or not to ignore course or time conflicting course sections.
     *                                   Note that a section is both course and time conflicting with itself.
     *                                   See {@link cn.edu.sustech.cs307.dto.CourseSearchEntry#conflictCourseNames}
     * @param ignorePassed               whether or not to ignore the student's passed courses.
     * @param ignoreMissingPrerequisites whether or not to ignore courses with missing prerequisites.
     * @param pageSize                   the page size, effectively `limit pageSize`.
     *                                   It is the number of {@link cn.edu.sustech.cs307.dto.CourseSearchEntry}
     * @param pageIndex                  the page index, effectively `offset pageIndex * pageSize`.
     *                                   If the page index is so large that there is no message,return an empty list
     * @return a list of search entries. See {@link cn.edu.sustech.cs307.dto.CourseSearchEntry}
     */
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


    /**
     * It is the course selection function according to the studentId and courseId.
     * The test case can be invalid data or conflict info,
     * so that it can return 8 different types of enroll results.
     *
     * It is possible for a student-course have ALREADY_SELECTED and ALREADY_PASSED or PREREQUISITES_NOT_FULFILLED.
     * Please make sure the return priority is the same as above in similar cases.
     * {@link cn.edu.sustech.cs307.service.StudentService.EnrollResult}
     *
     * To check whether prerequisite courses are available for current one,
     * only check the grade of prerequisite courses are >= 60 or PASS
     *
     * @param studentId
     * @param sectionId the id of CourseSection
     * @return See {@link cn.edu.sustech.cs307.service.StudentService.EnrollResult}
     */
    @Override
    public EnrollResult enrollCourse(int studentId, int sectionId) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement get_course_section = connection.prepareStatement(
                     "select courseId, semesterId, leftCapacity from courseSections where id = (?)");
             PreparedStatement check_student_section = connection.prepareStatement(
                     "select * from students_sections where studentId = (?) and sectionId = (?) and semesterId = (?);");
             PreparedStatement check_passed_the_course = connection.prepareStatement(
                     "select grade from students_course_grade where studentId = (?) and courseId = (?);");
        ) {
            /*TODO:
                1.找不找得到对应的 course section           COURSE_NOT_FOUND
                2.是否当前学期是否已经 enroll 过该 course section  （可以接受重修      ALREADY_ENROLLED
                3.学生已经通过了该 course section 对应的 course         ALREADY_PASSED
                4.学生没通过 或者 没学过 该 course 的 prerequisite      PREREQUISITES_NOT_FULFILLED
                5.该 course section 的 classes 对应的时间 学生还有别的class          COURSE_CONFLICT_FOUND
                  或者 学生已经 enroll了 该 course section 的 course 的其他 section
                6.该 course section 是否还有余量            COURSE_IS_FULL
                7. unknown error
                8. success.
             */
            get_course_section.setInt(1,sectionId);
            ResultSet selected_section_set = get_course_section.executeQuery();
            if (!selected_section_set.next()) {
                return EnrollResult.COURSE_NOT_FOUND;
            }
            String courseId = selected_section_set.getString(1);
            int semesterId = selected_section_set.getInt(2);
            int leftCapacity = selected_section_set.getInt(3);

            check_student_section.setInt(1,studentId);
            check_student_section.setInt(2,sectionId);
            check_student_section.setInt(3,semesterId);
            ResultSet student_section_set = check_student_section.executeQuery();
            if (student_section_set.next()){
                return EnrollResult.ALREADY_ENROLLED;
            }

            check_passed_the_course.setInt(1,studentId);
            check_passed_the_course.setString(2,courseId);
            ResultSet score_result = check_passed_the_course.executeQuery();
            score_result.next();
            short score = score_result.getShort(1);
            if (score >= 60){
                return EnrollResult.ALREADY_PASSED;
            }


        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }


    /**
     * Drop a course section for a student
     *
     * @param studentId
     * @param sectionId
     * @throws IllegalStateException if the student already has a grade for the course section.
     */
    @Override
    public void dropCourse(int studentId, int sectionId) throws IllegalStateException {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement get_course_section = connection.prepareStatement(
                     "select courseId, semesterId from courseSections where id = (?);");
             PreparedStatement check_grade = connection.prepareStatement(
                     "select grade from students_course_grade where studentId = (?) and courseId = (?);");
             PreparedStatement drop_section = connection.prepareStatement(
                     "delete from students_sections where studentId = (?) and sectionId = (?) and semesterId = (?);")
        ) {
            get_course_section.setInt(1,sectionId);
            ResultSet section_result = get_course_section.executeQuery();
            section_result.next();
            String courseId = section_result.getString(1);
            int semesterId = section_result.getInt(2);

            check_grade.setInt(1,studentId);
            check_grade.setString(2,courseId);
            ResultSet grade_result = check_grade.executeQuery();
            grade_result.next();
            short grade = grade_result.getShort(1);
            if (grade != -1){
                throw new IllegalStateException();
            }


            drop_section.setInt(1,studentId);
            drop_section.setInt(2,sectionId);
            drop_section.setInt(3,semesterId);
            drop_section.execute();
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }


    /**
     * It is used for importing existing data from other sources.
     * <p>
     * With this interface, staff for teaching affairs can bypass the
     * prerequisite fulfillment check to directly enroll a student in a course
     * and assign him/her a grade.
     *
     * If the scoring scheme of a course is one type in pass-or-fail and hundredmark grade,
     * your system should not accept the other type of grade.
     *
     * Course section's left capacity should remain unchanged after this method.
     *
     * @param studentId
     * @param sectionId We will get the sectionId of one section first
     *                  and then invoke the method by using the sectionId.
     * @param grade     Can be null
     *                  /////if it is null, then it means it will put unknown error when it is enrolled because we don't know the grade
     */
    @Override
    public void addEnrolledCourseWithGrade(int studentId, int sectionId, @Nullable Grade grade) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement getCourseGrade = connection.prepareStatement(
                     "select gradeType from students_course_grade where studentId = (?) and courseId = (?)");
             PreparedStatement setGrade = connection.prepareStatement(
                     "insert into students_course_grade (studentId, courseId, gradeType, grade) values (?,?,?,?);")
        ) {
            Course course = new MyCourseService().getCourseBySection(sectionId);
            String courseId = course.id;
            getCourseGrade.setInt(1,studentId);
            getCourseGrade.setString(2,courseId);
            ResultSet resultSet = getCourseGrade.executeQuery();
            String gradeType;
            short score;
            if (resultSet.next()) {
                return;
            }
            else {
                if (grade instanceof HundredMarkGrade){
                    gradeType = "HundredMarkGrade";
                    score = ((HundredMarkGrade) grade).mark;
                }
                else if (grade instanceof PassOrFailGrade){
                    gradeType = "PassOrFailGrade";
                    boolean pass = grade == PassOrFailGrade.PASS;
                    if (pass){
                        score = 60;
                    }
                    else {
                        score = 0;
                    }
                }
                else {
                    gradeType = "Unknown";
                    score = -1;
                }
            }

            setGrade.setInt(1,studentId);
            setGrade.setString(2,courseId);
            setGrade.setString(3,gradeType);
            setGrade.setShort(4,score);
            setGrade.execute();

        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }


    /**
     * For teachers to give students grade.
     *
     * @param studentId student id is in database
     * @param sectionId section id in test cases that have selected by the student
     * @param grade     a new grade
     */
    @Override
    public void setEnrolledCourseGrade(int studentId, int sectionId, Grade grade) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement getGradeType = connection.prepareStatement(
                     "select gradeType from students_course_grade where studentId = (?) and courseId = (?);"
             );
             PreparedStatement getCourseGrade = connection.prepareStatement(
                     "select gradeType from students_course_grade where studentId = (?) and courseId = (?)"
             );
             PreparedStatement setGarde = connection.prepareStatement(
                     "update students_course_grade set gradeType = (?), grade = (?) where studentId = (?) and courseId = (?)")
        ) {
            Course course = new MyCourseService().getCourseBySection(sectionId);
            String courseId = course.id;
            getCourseGrade.setInt(1,studentId);
            getCourseGrade.setString(2,courseId);
            ResultSet resultSet = getCourseGrade.executeQuery();
            resultSet.next();
            String gradeType = resultSet.getString(1);

            if (grade instanceof HundredMarkGrade && !gradeType.equals("PassOrFailGrade")){
                setGarde.setString(1,"HundredMarkGrade");
                setGarde.setShort(2,((HundredMarkGrade) grade).mark);
            }
            else if (grade instanceof PassOrFailGrade && gradeType.equals("HundredMarkGrade")){
                short score;
                boolean pass = grade == PassOrFailGrade.PASS;
                if (pass){
                    score = 60;
                }
                else {
                    score = 0;
                }
                setGarde.setString(1,"PassOrFailGrade");
                setGarde.setShort(2,score);
            }
            else {
                throw new IntegrityViolationException();
            }

            setGarde.setInt(1,studentId);
            setGarde.setString(2,courseId);
            setGarde.execute();

        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }


    /**
     * Queries grades of all enrolled courses in the given semester for the given student
     *
     * If a student selected one course for over one times, for example
     * failed the course and passed it in the next semester,
     * in the {@Code Map<Course, Grade>}, it only record the latest grade.
     *
     * @param studentId
     * @param semesterId the semester id, null means return all semesters' result.
     * @return A map from enrolled courses to corresponding grades.
     * If the grade is a hundred-mark score, the value should be wrapped by a
     * {@code HundredMarkGrade} object.
     * If the grade is pass or fail, the value should be {@code PassOrFailGrade.PASS}
     * or {@code PassOrFailGrade.FAIL} respectively.
     * If the grade is not set yet, the value should be null.
     */
    @Override
    public Map<Course, Grade> getEnrolledCoursesAndGrades(int studentId, @Nullable Integer semesterId) {
        Map<Course, Grade> courseGradeMap = new HashMap<>();
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("");
             PreparedStatement second_query = connection.prepareStatement("")
        ) {

        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }


    /**
     * Return a course table in current week according to the date.
     *
     * @param studentId
     * @param date
     * @return the student's course table for the entire week of the date.
     * Regardless which day of week the date is, return Monday-to-Sunday course table for that week.
     */
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


    /**
     * check whether a student satisfy a certain course's prerequisites.
     *
     * @param studentId
     * @param courseId
     * @return true if the student has passed the course's prerequisites (>=60 or PASS).
     */
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
             PreparedStatement first_query = connection.prepareStatement("select majorId from Students where id = (?)");
             PreparedStatement second_query = connection.prepareStatement("select * from Majors where id = ?")
        ) {
            first_query.setInt(1,studentId);
            ResultSet resultSet = first_query.executeQuery();
            resultSet.next();
            int majorId = resultSet.getInt(1);

            second_query.setInt(1,majorId);
            ResultSet result = second_query.executeQuery();
            result.next();
            Major major = new Major();
            major.id = majorId;
            major.name = result.getString(2);

            int departmentId = resultSet.getInt(3);
            major.department = new MyDepartmentService().getDepartment(departmentId);

            return major;
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }
}
