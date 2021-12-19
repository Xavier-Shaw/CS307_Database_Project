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
    public List<CourseSearchEntry> searchCourse(
            int studentId, int semesterId,
            @Nullable String searchCid, @Nullable String searchName, @Nullable String searchInstructor,
            @Nullable DayOfWeek searchDayOfWeek, @Nullable Short searchClassTime, @Nullable List<String> searchClassLocations,
            CourseType searchCourseType,
            boolean ignoreFull, boolean ignoreConflict, boolean ignorePassed, boolean ignoreMissingPrerequisites, int pageSize, int pageIndex) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("");
             PreparedStatement second_query = connection.prepareStatement("")
        ) {
            List<CourseSearchEntry> courseSearchEntries = new ArrayList<>();
            return courseSearchEntries;
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
                     "select \"courseId\", \"semesterId\", \"leftCapacity\" from courseSections where id = (?)");
             PreparedStatement check_student_section = connection.prepareStatement(
                     "select \"sectionId\" from students_sections where \"studentId\" = (?) and \"courseId\" = (?) and \"semesterId\" = (?);");
             PreparedStatement check_passed_the_course = connection.prepareStatement(
                     "select grade from students_course_grade where \"studentId\" = (?) and \"courseId\" = (?);");
             PreparedStatement enrollCourse = connection.prepareStatement(
                     "insert into students_sections (\"studentId\", \"semesterId\", \"courseId\", \"sectionId\") values (?,?,?,?);"
             );
             PreparedStatement fix_capacity = connection.prepareStatement(
                     "update \"courseSections\" set \"leftCapacity\" = \"leftCapacity\" - 1 where id = (?);"
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
            get_course_section.setInt(1,sectionId);
            ResultSet selected_section_set = get_course_section.executeQuery();
            if (!selected_section_set.next()) {
                return EnrollResult.COURSE_NOT_FOUND;
            }
            String courseId = selected_section_set.getString(1);
            int semesterId = selected_section_set.getInt(2);
            int leftCapacity = selected_section_set.getInt(3);

            //// check already_enroll
            check_student_section.setInt(1,studentId);
            check_student_section.setInt(2,sectionId);
            check_student_section.setInt(3,semesterId);
            ResultSet student_section_set = check_student_section.executeQuery();
            boolean course_conflict_by_course = false;
            while (student_section_set.next()){
                int enrolled_sectionId = student_section_set.getInt(1);
                if (enrolled_sectionId == sectionId) {
                    return EnrollResult.ALREADY_ENROLLED;
                }
                else {
                    course_conflict_by_course = true;
                }
            }

            //////check already pass
            check_passed_the_course.setInt(1,studentId);
            check_passed_the_course.setString(2,courseId);
            ResultSet score_result = check_passed_the_course.executeQuery();
            score_result.next();
            short score = score_result.getShort(1);
            if (score >= 60){
                return EnrollResult.ALREADY_PASSED;
            }

            //////check prerequisite fulfilled
            if (!passedPrerequisitesForCourse(studentId,courseId)){
                return EnrollResult.PREREQUISITES_NOT_FULFILLED;
            }

            ////// check course conflict
            if (course_conflict_by_course){
                return EnrollResult.COURSE_CONFLICT_FOUND;
            }

            List<CourseSectionClass> classes_of_this_section = new MyCourseService().getCourseSectionClasses(sectionId);
            for (CourseSectionClass section_class :
                    classes_of_this_section) {
                int week_first = (int) section_class.weekList.toArray()[0];
                int week_second = (int) section_class.weekList.toArray()[1];
                Calendar calendar = Calendar.getInstance();
                Semester semester = new MySemesterService().getSemester(semesterId);
                calendar.setTime(semester.begin);
                int cnt = 0;
                java.util.Date date1 = null;
                java.util.Date date2;
                while (true){
                    calendar.add(Calendar.DAY_OF_WEEK,1);
                    cnt ++;
                    if (cnt == week_first){
                        date1 = calendar.getTime();
                    }
                    if (cnt == week_second){
                        date2 = calendar.getTime();
                        break;
                    }
                }
                CourseTable first_week = getCourseTable(studentId, (Date) date1);
                for (CourseTable.CourseTableEntry c:
                        first_week.table.get(section_class.dayOfWeek)) {
                    if (!(c.classBegin >= section_class.classEnd || c.classEnd <= section_class.classBegin)){
                        return EnrollResult.COURSE_CONFLICT_FOUND;
                    }
                }
                CourseTable second_week = getCourseTable(studentId, (Date) date2);
                for (CourseTable.CourseTableEntry c:
                        second_week.table.get(section_class.dayOfWeek)) {
                    if (!(c.classBegin >= section_class.classEnd || c.classEnd <= section_class.classBegin)){
                        return EnrollResult.COURSE_CONFLICT_FOUND;
                    }
                }
            }

            ////// check course is full
            if (leftCapacity == 0){
                return EnrollResult.COURSE_IS_FULL;
            }

            /////// success
            enrollCourse.setInt(1,studentId);
            enrollCourse.setInt(2,semesterId);
            enrollCourse.setString(3,courseId);
            enrollCourse.setInt(4,sectionId);
            enrollCourse.execute();

            fix_capacity.setInt(1,sectionId);
            fix_capacity.execute();
            return EnrollResult.SUCCESS;

        } catch (SQLException e) {
            return EnrollResult.UNKNOWN_ERROR;
        }
    }


    public void checkGrade(int studentId, String courseId){
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement getGrade = connection.prepareStatement(
                    "select grade from students_course_grade where \"studentId\" = (?) and \"courseId\" = (?);"
            )
        ) {
            getGrade.setInt(1,studentId);
            getGrade.setString(2,courseId);
            ResultSet grade_result = getGrade.executeQuery();
            if (grade_result.next()) {
                short grade = grade_result.getShort(1);
                if (grade != -1) {
                    throw new IllegalStateException();
                }
            }
        } catch (SQLException ignored) {
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
                     "select \"courseId\", \"semesterId\" from \"courseSections\" where id = (?);");

             PreparedStatement drop_section = connection.prepareStatement(
                     "delete from students_sections where \"studentId\" = (?) and \"sectionId\" = (?) and \"semesterId\" = (?);")
        ) {
            get_course_section.setInt(1,sectionId);
            ResultSet section_result = get_course_section.executeQuery();
            section_result.next();
            String courseId = section_result.getString(1);
            int semesterId = section_result.getInt(2);

            checkGrade(studentId,courseId);

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
                     "select \"gradeType\" from students_course_grade where \"studentId\" = (?) and \"courseId\" = (?)");
             PreparedStatement setGrade = connection.prepareStatement(
                     "insert into students_course_grade (\"studentId\", \"courseId\", \"gradeType\", grade) values (?,?,?,?);")
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
                     "select \"gradeType\" from students_course_grade where \"studentId\" = (?) and \"courseId\" = (?);"
             );
             PreparedStatement getCourseGrade = connection.prepareStatement(
                     "select \"gradeType\" from students_course_grade where \"studentId\" = (?) and \"courseId\" = (?)"
             );
             PreparedStatement setGarde = connection.prepareStatement(
                     "update students_course_grade set \"gradeType\" = (?), grade = (?) where \"studentId\" = (?) and \"courseId\" = (?)")
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
        Map<Course, Grade> course_grade_Map = new HashMap<>();
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement get_student_sections_by_semester = connection.prepareStatement(
                     "select \"sectionId\" from students_sections where \"studentId\" = (?) and \"semesterId\" = (?);");
             PreparedStatement getGrade = connection.prepareStatement(
                     "select \"gradeType\", grade from students_course_grade where \"studentId\" = (?) and \"courseId\" = (?);");
             PreparedStatement getAllGrade = connection.prepareStatement(
                     "select \"courseId\", \"gradeType\", grade from students_course_grade where \"studentId\" = (?);"
             )
        ) {
            get_student_sections_by_semester.setInt(1,studentId);
            if (semesterId != null) {
                get_student_sections_by_semester.setInt(2, semesterId);
                ResultSet sections = get_student_sections_by_semester.executeQuery();
                while (sections.next()){
                    int sectionId = sections.getInt(1);
                    Course course = new MyCourseService().getCourseBySection(sectionId);
                    getGrade.setInt(1, studentId);
                    getGrade.setString(2, course.id);
                    ResultSet gradeSet = getGrade.executeQuery();
                    gradeSet.next();
                    String gradeType = gradeSet.getString(1);
                    short score = gradeSet.getShort(2);
                    Grade grade;
                    if (gradeType.equals("HundredMarkGrade")){
                        grade = new HundredMarkGrade(score);
                    }
                    else if (gradeType.equals("PassOrFailGrade")){
                        if (score == 60){
                            grade = PassOrFailGrade.PASS;
                        }
                        else {
                            grade = PassOrFailGrade.FAIL;
                        }
                    }
                    else {
                        grade = null;
                    }
                    course_grade_Map.put(course,grade);
                }
            }
            else {
                getAllGrade.setInt(1,studentId);
                ResultSet allGrades = getAllGrade.executeQuery();
                while (allGrades.next()){
                    String courseId = allGrades.getString(1);
                    Course course = new MyCourseService().getCourseByCourseId(courseId);
                    String gradeType = allGrades.getString(2);
                    short score = allGrades.getShort(3);
                    Grade grade;
                    if (gradeType.equals("HundredMarkGrade")){
                        grade = new HundredMarkGrade(score);
                    }
                    else if (gradeType.equals("PassOrFailGrade")){
                        if (score == 60){
                            grade = PassOrFailGrade.PASS;
                        }
                        else {
                            grade = PassOrFailGrade.FAIL;
                        }
                    }
                    else {
                        grade = null;
                    }

                    course_grade_Map.put(course,grade);
                }
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
             PreparedStatement getSemester = connection.prepareStatement(
                     "select id, begin_date from Semesters where begin_date <= (?) and end_date >= (?);");
             PreparedStatement get_student_sections = connection.prepareStatement(
                     "select \"sectionId\" from students_sections where \"studentId\" = (?) and \"semesterId\" = (?);");
             PreparedStatement get_section = connection.prepareStatement(
                     "select \"courseId\", \"sectionName\" from courseSections where \"sectionId\" = (?);"
             )
        ) {
            getSemester.setDate(1,date);
            getSemester.setDate(2,date);
            ResultSet res_sem = getSemester.executeQuery();
            res_sem.next();
            int semesterId = res_sem.getInt(1);
            Date begin = res_sem.getDate(2);

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(begin);
            short week = 1;
            while (true){
                calendar.add(Calendar.DAY_OF_WEEK,1);
                if (calendar.after(date)){
                    break;
                }
                else {
                    week++;
                }
            }

            get_student_sections.setInt(1,studentId);
            get_student_sections.setInt(2,semesterId);
            ResultSet section_set = get_student_sections.executeQuery();
            while (section_set.next()){
                int sectionId = section_set.getInt(1);
                get_section.setInt(1,sectionId);
                ResultSet section_res = get_section.executeQuery();
                section_res.next();
                String courseId = section_res.getString(1);
                String sectionName = section_res.getString(2);
                Course course = new MyCourseService().getCourseByCourseId(courseId);
                String courseName = course.name;
                List<CourseSectionClass> classes = new MyCourseService().getCourseSectionClasses(sectionId);
                for (CourseSectionClass courseSectionClass:
                        classes) {
                    if (courseSectionClass.weekList.contains(week)) {
                        CourseTable.CourseTableEntry courseTableEntry = new CourseTable.CourseTableEntry();
                        courseTableEntry.courseFullName = String.format("%s[%s]", courseName, sectionName);
                        courseTableEntry.instructor = courseSectionClass.instructor;
                        courseTableEntry.classBegin = courseSectionClass.classBegin;
                        courseTableEntry.classEnd = courseSectionClass.classEnd;
                        courseTableEntry.location = courseSectionClass.location;
                        switch (courseSectionClass.dayOfWeek){
                            case MONDAY:
                                MONDAY_Set.add(courseTableEntry);
                                break;
                            case TUESDAY:
                                TUESDAY_Set.add(courseTableEntry);
                                break;
                            case WEDNESDAY:
                                WEDNESDAY_Set.add(courseTableEntry);
                                break;
                            case THURSDAY:
                                THURSDAY_Set.add(courseTableEntry);
                                break;
                            case FRIDAY:
                                FRIDAY_Set.add(courseTableEntry);
                                break;
                            case SATURDAY:
                                SATURDAY_Set.add(courseTableEntry);
                                break;
                            case SUNDAY:
                                SUNDAY_Set.add(courseTableEntry);
                        }
                    }
                }
            }

            courseTable.table.put(DayOfWeek.MONDAY,MONDAY_Set);
            courseTable.table.put(DayOfWeek.TUESDAY,TUESDAY_Set);
            courseTable.table.put(DayOfWeek.WEDNESDAY,WEDNESDAY_Set);
            courseTable.table.put(DayOfWeek.THURSDAY,THURSDAY_Set);
            courseTable.table.put(DayOfWeek.FRIDAY,FRIDAY_Set);
            courseTable.table.put(DayOfWeek.SATURDAY,SATURDAY_Set);
            courseTable.table.put(DayOfWeek.SUNDAY,SUNDAY_Set);
            return courseTable;
        } catch (SQLException e) {
            return courseTable;
        }
    }


    public boolean getGrade(int studentId, int listId){
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement getAtomCourseId = connection.prepareStatement(
                     "select \"courseId\" from AtomPrerequisites where \"listId\" = (?)");
             PreparedStatement getGrade = connection.prepareStatement(
                     "select grade from students_course_grade where \"studentId\" = (?) and \"courseId\" = (?);")
        ) {
            getAtomCourseId.setInt(1,listId);
            ResultSet resultSet = getAtomCourseId.executeQuery();
            resultSet.next();
            String courseId = resultSet.getString(1);

            getGrade.setInt(1, studentId);
            getGrade.setString(2, courseId);
            ResultSet gradeSet = getGrade.executeQuery();
            gradeSet.next();
            short score = gradeSet.getShort(2);
            return score >= 60;
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }


    public boolean handlePrerequisite(int studentId, int listId){
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement getType = connection.prepareStatement(
                     "select type from prerequisite_list where id = (?)");
             PreparedStatement getTerms = connection.prepareStatement(
                     "select terms from ? where \"listId\" = (?)");
        ) {
            getType.setInt(1,listId);
            ResultSet res = getType.executeQuery();
            res.next();
            String type = res.getString(1);
            /**
             * IDEA : divide the prerequisites into AtomPrerequisites to get if it is passed or not
             *
             * if the prerequisite we are handling now is a AtomPrerequisite,
             * then the result of whether it is passed can be determined by the method getGrade()
             *
             * if the prerequisite we are handling now is a AndPrerequisite,
             * then the result of whether it is passed or not depends on the && logic of its son_prerequisites
             * because it can be judged as passed if and only if ALL of its son_prerequisites are passed
             *
             * if the prerequisite we are handling now is a OrPrerequisite,
             * then the result of whether it is passed or not depends on the || logic of its son_prerequisites
             * because it can be judged as passed if one of its son_prerequisite is passed
             */
            if (type.equals("ATOM")){
                return getGrade(studentId,listId);
            }
            else if (type.equals("AND")){
                getTerms.setString(1,"AndPrerequisites");
                getTerms.setInt(2,listId);
                ResultSet resultSet = getTerms.executeQuery();
                resultSet.next();
                Array array = resultSet.getArray(1);
                ResultSet set = array.getResultSet();
                boolean passed = true;
                while (set.next()){
                    passed = passed && handlePrerequisite(studentId, set.getInt(2));
                }
                return passed;
            }
            else {
                getTerms.setString(1,"OrPrerequisites");
                getTerms.setInt(2,listId);
                ResultSet resultSet = getTerms.executeQuery();
                resultSet.next();
                Array array = resultSet.getArray(1);
                ResultSet set = array.getResultSet();
                boolean passed = false;
                while (set.next()){
                    passed = passed || handlePrerequisite(studentId, set.getInt(2));
                }
                return passed;
            }
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
             PreparedStatement get_root_prerequisite = connection.prepareStatement(
                     "select root_prerequisite from \"Courses\" where \"courseId\" = (?);");

        ) {
            get_root_prerequisite.setString(1,courseId);
            ResultSet resultSet = get_root_prerequisite.executeQuery();
            resultSet.next();
            int root_id = resultSet.getInt(1);
            if (root_id == 0){
                return true;
            }

            return handlePrerequisite(studentId,root_id);

        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }

    @Override
    public Major getStudentMajor(int studentId) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("select \"majorId\" from Students where id = (?)");
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


    public Student getStudentById(int studentId){
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement getStudent = connection.prepareStatement(
                    "select \"majorId\", \"firstName\", \"lastName\", \"enrolledDate\" from \"Students\" where \"userId\" = (?);"
            )
        ){
            getStudent.setInt(1, studentId);
            ResultSet studentSet = getStudent.executeQuery();
            studentSet.next();

            int majorId = studentSet.getInt(1);
            String firstName = studentSet.getString(2);
            String lastName = studentSet.getString(3);
            Date enrolledDate = studentSet.getDate(4);
            Major major = new MyMajorService().getMajor(majorId);
            byte judge = (byte) firstName.charAt(0);
            String fullName;
            if ( (judge >= 65 && judge <= 90) || (judge >= 97 && judge <= 122)) {
                fullName = firstName + " " + lastName;
            }
            else {
                fullName = firstName + lastName;
            }
            Student student = new Student();
            student.id = studentId;
            student.major = major;
            student.fullName = fullName;
            student.enrolledDate = enrolledDate;
            return student;
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }
}
