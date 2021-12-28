package me.x.impls.services;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.*;
import cn.edu.sustech.cs307.dto.grade.Grade;
import cn.edu.sustech.cs307.dto.grade.HundredMarkGrade;
import cn.edu.sustech.cs307.dto.grade.PassOrFailGrade;
import cn.edu.sustech.cs307.dto.prerequisite.Prerequisite;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.StudentService;

import javax.annotation.Nullable;
import java.sql.*;
import java.sql.Date;
import java.time.DayOfWeek;
import java.util.*;

public class MyStudentService implements StudentService {


    /**
     * Add one student according to following parameters.
     * If some parameters are invalid, throw {@link cn.edu.sustech.cs307.exception.IntegrityViolationException}
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
                     "insert into \"Students\" (\"userId\", \"majorId\", \"firstName\", \"lastName\", \"enrolledDate\") values (?,?,?,?,?); ");
             PreparedStatement stmt = connection.prepareStatement(
                     "insert into \"Users\" (\"userId\", \"firstName\", \"lastName\") values (?,?,?);"
             )
        ) {
            first_query.setInt(1, userId);
            first_query.setInt(2, majorId);
            first_query.setString(3, firstName);
            first_query.setString(4, lastName);
            first_query.setDate(5, enrolledDate);
            first_query.execute();

            stmt.setInt(1, userId);
            stmt.setString(2, firstName);
            stmt.setString(3, lastName);
            stmt.execute();
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
    public synchronized List<CourseSearchEntry> searchCourse(
            int studentId, int semesterId, @Nullable String searchCid,
            @Nullable String searchName, @Nullable String searchInstructor, @Nullable DayOfWeek searchDayOfWeek,
            @Nullable Short searchClassTime, @Nullable List<String> searchClassLocations, CourseType searchCourseType,
            boolean ignoreFull, boolean ignoreConflict, boolean ignorePassed, boolean ignoreMissingPrerequisites,
            int pageSize, int pageIndex) {
        ArrayList<CourseSearchEntry> arrayList = new ArrayList<>();
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement stmt = connection.prepareStatement(
                     "select * from searchCourse(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);")
        ) {
            stmt.setInt(1, studentId);
            stmt.setInt(2, semesterId);
            if (searchCid != null) {
                stmt.setString(3, searchCid);
            } else {
                stmt.setNull(3, Types.VARCHAR);
            }
            if (searchName != null) {
                stmt.setString(4, searchName);
            } else {
                stmt.setNull(4, Types.VARCHAR);
            }
            if (searchInstructor != null) {
                stmt.setString(5, searchInstructor);
            } else {
                stmt.setNull(5, Types.VARCHAR);
            }
            if (searchDayOfWeek != null) {
                stmt.setString(6, searchDayOfWeek.toString());
            } else {
                stmt.setNull(6, Types.VARCHAR);
            }
            if (searchClassTime != null) {
                stmt.setShort(7, searchClassTime);
            } else {
                stmt.setNull(7, Types.SMALLINT);
            }
            if (searchClassLocations != null) {
                String[] strings = searchClassLocations.toArray(new String[0]);
                Array arr = connection.createArrayOf("varchar", strings);
                stmt.setArray(8, arr);
            } else {
                stmt.setNull(8, 0);
            }
            stmt.setString(9, searchCourseType.toString());
            stmt.setBoolean(10, ignoreFull);
            stmt.setBoolean(11, ignoreConflict);
            stmt.setBoolean(12, ignorePassed);
            stmt.setBoolean(13, ignoreMissingPrerequisites);
            stmt.setInt(14, pageSize);
            stmt.setInt(15, pageIndex);
            stmt.execute();
            ResultSet result = stmt.getResultSet();
            MyCourseService myCourseService = new MyCourseService();
            while (result.next()) {
                CourseSearchEntry courseSearchEntry = new CourseSearchEntry();
                courseSearchEntry.course = myCourseService.getCourseBySection(result.getInt(1));
                courseSearchEntry.section = new CourseSection();
                courseSearchEntry.section.id = result.getInt(1);
                courseSearchEntry.section.totalCapacity = result.getInt(6);
                courseSearchEntry.section.leftCapacity = result.getInt(7);
                courseSearchEntry.section.name = result.getString(5);
                courseSearchEntry.sectionClasses = new HashSet<>(myCourseService.getCourseSectionClasses(result.getInt(1)));
                courseSearchEntry.conflictCourseNames = new ArrayList<>();
                Array arr = result.getArray(13);
                if (arr != null) {
                    ResultSet rs = arr.getResultSet();
                    while (rs.next()) {
                        courseSearchEntry.conflictCourseNames.add(rs.getString(2));
                    }
                }
                arrayList.add(courseSearchEntry);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return arrayList;
    }


    /**
     * It is the course selection function according to the studentId and courseId.
     * The test case can be invalid data or conflict info,
     * so that it can return 8 different types of enroll results.
     * <p>
     * It is possible for a student-course have ALREADY_SELECTED and ALREADY_PASSED or PREREQUISITES_NOT_FULFILLED.
     * Please make sure the return priority is the same as above in similar cases.
     * {@link cn.edu.sustech.cs307.service.StudentService.EnrollResult}
     * <p>
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
                     "select \"leftCapacity\" from \"courseSections\" where id = (?)");
             PreparedStatement check_section_selected = connection.prepareStatement(
                     "select \"sectionId\" from course_select where \"studentId\" = (?) and \"sectionId\" = (?);");
             PreparedStatement check_passed_the_course = connection.prepareStatement(
                     "select check_course_passed(?,?)");
             PreparedStatement check_time = connection.prepareStatement(
                     "select get_time_bad(?,?) is null;"
             );
             PreparedStatement enrollCourse = connection.prepareStatement(
                     "insert into course_select (\"studentId\", \"sectionId\") values (?,?);" +
                     " update \"courseSections\" set \"leftCapacity\" = \"leftCapacity\" - 1 where id = (?);"
             )
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

            //// check course not found
            get_course_section.setInt(1, sectionId);
            ResultSet selected_section_set = get_course_section.executeQuery();
            if (!selected_section_set.next()) {
                return EnrollResult.COURSE_NOT_FOUND;
            }
            int leftCapacity = selected_section_set.getInt(1);


            //// check already_enroll
            check_section_selected.setInt(1, studentId);
            check_section_selected.setInt(2, sectionId);

            boolean a = check_section_selected.execute();
            if (a) {
                ResultSet student_section_set = check_section_selected.getResultSet();
                if (student_section_set.next()) {
                        return EnrollResult.ALREADY_ENROLLED;
                }
            }


            //////check already pass
            check_passed_the_course.setInt(1, sectionId);
            check_passed_the_course.setInt(2, studentId);
            boolean b = check_passed_the_course.execute();
            if (b) {
                ResultSet score_result = check_passed_the_course.getResultSet();
                score_result.next();
                if (score_result.getBoolean(1)) {
                    return EnrollResult.ALREADY_PASSED;
                }
            }

            //////check prerequisite fulfilled
            if (!checkPrerequisite(studentId, sectionId)) {
                return EnrollResult.PREREQUISITES_NOT_FULFILLED;
            }


            ////// check course conflict
            check_time.setInt(1, studentId);
            check_time.setInt(2, sectionId);
            check_time.execute();
            ResultSet r = check_time.getResultSet();
            r.next();
            if (!r.getBoolean(1)) {
                return EnrollResult.COURSE_CONFLICT_FOUND;
            }


            ////// check course is full
            if (leftCapacity == 0) {
                return EnrollResult.COURSE_IS_FULL;
            }


            /////// success
            enrollCourse.setInt(1, studentId);
            enrollCourse.setInt(2, sectionId);
            enrollCourse.setInt(3, sectionId);
            enrollCourse.execute();

            return EnrollResult.SUCCESS;

        } catch (SQLException e) {
            return EnrollResult.UNKNOWN_ERROR;
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
             PreparedStatement drop_section = connection.prepareStatement(
                     "select drop_course(?,?);")
        ) {

            drop_section.setInt(1, studentId);
            drop_section.setInt(2, sectionId);
            drop_section.execute();
            ResultSet resultSet = drop_section.getResultSet();
            resultSet.next();
            if (!resultSet.getBoolean(1)){
                throw new IllegalStateException();
            }

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
     * <p>
     * If the scoring scheme of a course is one type in pass-or-fail and hundredmark grade,
     * your system should not accept the other type of grade.
     * <p>
     * Course section's left capacity should remain unchanged after this method.
     *
     * @param studentId
     * @param sectionId We will get the sectionId of one section first
     *                  and then invoke the method by using the sectionId.
     * @param grade     Can be null
     */
    @Override
    public void addEnrolledCourseWithGrade(int studentId, int sectionId, @Nullable Grade grade) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement updateGrade = connection.prepareStatement(
                     "select add_course_with_grade(?,?,?,?);");
             PreparedStatement setGrade = connection.prepareStatement(
                     "insert into course_select (\"studentId\", \"sectionId\") values (?,?);")
        ) {

            if (grade != null) {
                String gradeType;
                short score ;
                if (grade instanceof HundredMarkGrade) {
                    gradeType = "HundredMarkGrade";
                    score = ((HundredMarkGrade) grade).mark;
                }
                else {
                    gradeType = "PassOrFailGrade";
                    boolean pass = grade == PassOrFailGrade.PASS;
                    if (pass) {
                        score = 60;
                    } else {
                        score = 0;
                    }
                }
                updateGrade.setInt(1,studentId);
                updateGrade.setInt(2,sectionId);
                updateGrade.setString(3,gradeType);
                updateGrade.setShort(4,score);
                updateGrade.execute();
            }
            else {
                setGrade.setInt(1, studentId);
                setGrade.setInt(2, sectionId);
                setGrade.execute();
            }

        } catch (SQLException e) {
            e.printStackTrace();
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
             PreparedStatement setGarde = connection.prepareStatement(
                     "update course_select set \"gradeType\" = (?) and grade = (?) where \"studentId\" = (?) and \"sectionId\" = (?)")
        ) {
            if (grade instanceof HundredMarkGrade) {
                setGarde.setString(1, "HundredMarkGrade");
                setGarde.setShort(2, ((HundredMarkGrade) grade).mark);
            }
            else if (grade instanceof PassOrFailGrade) {
                short score;
                boolean pass = (grade == PassOrFailGrade.PASS);
                if (pass) {
                    score = 60;
                } else {
                    score = 0;
                }
                setGarde.setString(1, "PassOrFailGrade");
                setGarde.setShort(2, score);
            }

            setGarde.setInt(3, studentId);
            setGarde.setInt(4, sectionId);
            setGarde.execute();

        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }


    /**
     * Queries grades of all enrolled courses in the given semester for the given student
     * <p>
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
        Map<Course, Grade> course_grade_Map = new HashMap<>();
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement get_course_and_grade_in_semester = connection.prepareStatement(
                     "select get_course_and_grade_in_semester(?,?);");
             PreparedStatement getAllGrade = connection.prepareStatement(
                     "select get_all_course_and_grade(?);"
             )
        ) {
            ResultSet res;
            if (semesterId != null) {
                get_course_and_grade_in_semester.setInt(1, studentId);
                get_course_and_grade_in_semester.setInt(2, semesterId);
                res = get_course_and_grade_in_semester.executeQuery();
            } else {
                getAllGrade.setInt(1, studentId);
                res = getAllGrade.executeQuery();
            }

            while (res.next()) {
                Course course = new Course();
                course.id = res.getString(1);
                course.name = res.getString(2);
                course.credit = res.getInt(3);
                course.classHour = res.getInt(4);
                String grading = res.getString(5);
                if (grading.equals("HundredMarkGrade")) {
                    course.grading = Course.CourseGrading.HUNDRED_MARK_SCORE;
                } else {
                    course.grading = Course.CourseGrading.PASS_OR_FAIL;
                }

                String gradeType = res.getString(6);
                short score = res.getShort(7);
                Grade grade;
                if (gradeType.equals("HundredMarkGrade")) {
                    grade = new HundredMarkGrade(score);
                } else if (gradeType.equals("PassOrFailGrade")) {
                    if (score == 60) {
                        grade = PassOrFailGrade.PASS;
                    } else {
                        grade = PassOrFailGrade.FAIL;
                    }
                } else {
                    grade = null;
                }

                course_grade_Map.put(course, grade);
            }

            return course_grade_Map;
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
        CourseTable courseTable = new CourseTable();
        Set<CourseTable.CourseTableEntry> MONDAY_Set = new HashSet<>();
        Set<CourseTable.CourseTableEntry> TUESDAY_Set = new HashSet<>();
        Set<CourseTable.CourseTableEntry> WEDNESDAY_Set = new HashSet<>();
        Set<CourseTable.CourseTableEntry> THURSDAY_Set = new HashSet<>();
        Set<CourseTable.CourseTableEntry> FRIDAY_Set = new HashSet<>();
        Set<CourseTable.CourseTableEntry> SATURDAY_Set = new HashSet<>();
        Set<CourseTable.CourseTableEntry> SUNDAY_Set = new HashSet<>();

        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement getCourseTable = connection.prepareStatement(
                     "select get_course_table(?,?);");
        ) {
            getCourseTable.setInt(1,studentId);
            getCourseTable.setDate(2, date);
            ResultSet res = getCourseTable.executeQuery();

            while (res.next()) {
                int week = res.getInt(11);
                boolean in_this_week = false;
                Array array = res.getArray(10);
                ResultSet rs = array.getResultSet();
                while (rs.next()) {
                    int cur_week = (rs.getInt(2));
                    if (week == cur_week) {
                        in_this_week = true;
                        break;
                    }
                    else if (cur_week > week){
                        break;
                    }
                }

                if (!in_this_week) {
                    continue;
                }

                String courseName = res.getString(1);
                String sectionName = res.getString(2);

                int instructorId = res.getInt(3);
                String firstName = res.getString(4);
                String lastName = res.getString(5);
                Instructor instructor = new Instructor();
                instructor.id = instructorId;
                byte judge = (byte) firstName.charAt(0);
                if ( (judge >= 65 && judge <= 90) || (judge >= 97 && judge <= 122)) {
                    instructor.fullName = firstName + " " + lastName;
                }
                else {
                    instructor.fullName = firstName + lastName;
                }

                CourseTable.CourseTableEntry courseTableEntry = new CourseTable.CourseTableEntry();
                courseTableEntry.courseFullName = String.format("%s[%s]", courseName, sectionName);
                courseTableEntry.instructor = instructor;
                courseTableEntry.classBegin = res.getShort(6);
                courseTableEntry.classEnd = res.getShort(7);
                courseTableEntry.location = res.getString(8);
                String dayOfWeek = res.getString(9);

                switch (dayOfWeek) {
                    case "MONDAY":
                        MONDAY_Set.add(courseTableEntry);
                        break;
                    case "TUESDAY":
                        TUESDAY_Set.add(courseTableEntry);
                        break;
                    case "WEDNESDAY":
                        WEDNESDAY_Set.add(courseTableEntry);
                        break;
                    case "THURSDAY":
                        THURSDAY_Set.add(courseTableEntry);
                        break;
                    case "FRIDAY":
                        FRIDAY_Set.add(courseTableEntry);
                        break;
                    case "SATURDAY":
                        SATURDAY_Set.add(courseTableEntry);
                        break;
                    case "SUNDAY":
                        SUNDAY_Set.add(courseTableEntry);
                }
            }

            courseTable.table.put(DayOfWeek.MONDAY, MONDAY_Set);
            courseTable.table.put(DayOfWeek.TUESDAY, TUESDAY_Set);
            courseTable.table.put(DayOfWeek.WEDNESDAY, WEDNESDAY_Set);
            courseTable.table.put(DayOfWeek.THURSDAY, THURSDAY_Set);
            courseTable.table.put(DayOfWeek.FRIDAY, FRIDAY_Set);
            courseTable.table.put(DayOfWeek.SATURDAY, SATURDAY_Set);
            courseTable.table.put(DayOfWeek.SUNDAY, SUNDAY_Set);
            return courseTable;
        } catch (SQLException e) {
            return courseTable;
        }
    }


    private boolean checkPrerequisite(int studentId, int sectionId) {
        try (Connection  connection=SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement stmt = connection.prepareStatement(("select check_prerequisite_by_csc_id(?,?);"))) {
            stmt.setInt(1, sectionId);
            stmt.setInt(2, studentId);
            stmt.execute();
            ResultSet resultSet = stmt.getResultSet();
            resultSet.next();
            return resultSet.getBoolean(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return true;
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
        try (Connection connection=SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement stmt = connection.prepareStatement("select id from \"courseSections\" where \"sectionName\" = (?);")
        ) {
            stmt.setString(1, courseId);
            stmt.execute();
            ResultSet resultSet = stmt.getResultSet();
            resultSet.next();
            int sectionId = resultSet.getInt(1);

            return checkPrerequisite(studentId,sectionId);
        } catch (SQLException e) {
           throw new EntityNotFoundException();
        }
    }


    @Override
    public Major getStudentMajor(int studentId) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("select \"majorId\" from \"Students\" where id = (?)");
             PreparedStatement second_query = connection.prepareStatement("select * from \"Majors\" where id = ?")
        ) {
            first_query.setInt(1, studentId);
            ResultSet resultSet = first_query.executeQuery();
            resultSet.next();
            int majorId = resultSet.getInt(1);

            second_query.setInt(1, majorId);
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
