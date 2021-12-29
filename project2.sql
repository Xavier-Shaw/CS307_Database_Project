create sequence "Departments_id_seq"
    as integer
    minvalue 0;

alter sequence "Departments_id_seq" owner to checker;

create table "Courses"
(
    id                serial,
    "courseId"        varchar not null
        constraint courses_pk
            primary key,
    "courseName"      varchar not null,
    credit            integer not null,
    "classHour"       integer not null,
    grading           varchar not null,
    root_prerequisite integer not null
);

alter table "Courses"
    owner to checker;

create table "Departments"
(
    id   integer default nextval('"Departments_id_seq"'::regclass) not null
        constraint departments_pk
            primary key,
    name varchar                                                   not null
);

alter table "Departments"
    owner to checker;

alter sequence "Departments_id_seq" owned by "Departments".id;

create table "Majors"
(
    id             serial
        constraint majors_pk
            primary key,
    name           varchar not null,
    "departmentId" integer not null
        constraint majors_departments_id_fk
            references "Departments"
            on delete cascade
);

alter table "Majors"
    owner to checker;

create table prerequisite_list
(
    id   serial
        constraint prerequisite_list_pk
            primary key,
    type varchar(15) not null
);

alter table prerequisite_list
    owner to checker;

create table "AtomPrerequisites"
(
    id         serial
        constraint atomprerequisites_pk
            primary key,
    "listId"   integer not null
        constraint atomprerequisites_prerequisite_list_id_fk
            references prerequisite_list
            on delete cascade,
    "courseId" varchar not null
        constraint atomprerequisites_courses__fk
            references "Courses"
            on delete cascade
);

alter table "AtomPrerequisites"
    owner to checker;

create table "AndPrerequisites"
(
    id       serial
        constraint andprerequisites_pk
            primary key,
    "listId" integer   not null
        constraint andprerequisites_prerequisite_list_id_fk
            references prerequisite_list
            on delete cascade,
    terms    integer[] not null
);

alter table "AndPrerequisites"
    owner to checker;

create table "OrPrerequisites"
(
    id       serial
        constraint orprerequisites_pk
            primary key,
    "listId" integer   not null
        constraint orprerequisites_fk
            references prerequisite_list
            on delete cascade,
    terms    integer[] not null
);

alter table "OrPrerequisites"
    owner to checker;

create table "Semesters"
(
    id         serial
        constraint semesters_pk
            primary key,
    name       varchar not null,
    begin_date date    not null,
    end_date   date    not null
);

alter table "Semesters"
    owner to checker;

create table "courseSections"
(
    id              serial
        constraint coursesections_pk
            primary key,
    "courseId"      varchar not null
        constraint coursesections_courses_c_fk
            references "Courses"
            on delete cascade,
    "semesterId"    integer not null
        constraint coursesections_semesters_id_fk
            references "Semesters"
            on delete cascade,
    "sectionName"   varchar not null,
    "totalCapacity" integer not null,
    "leftCapacity"  integer not null
);

alter table "courseSections"
    owner to checker;

create table "Users"
(
    "userId"    integer not null
        constraint users_pk
            primary key,
    "firstName" varchar not null,
    "lastName"  varchar not null
);

alter table "Users"
    owner to checker;

create table "courseSectionClasses"
(
    id             serial
        constraint coursesectionclasses_pk
            primary key,
    "sectionId"    integer   not null
        constraint coursesectionclasses_course__fk
            references "courseSections"
            on delete cascade,
    "instructorId" integer   not null
        constraint coursesectionclasses_users_userid_fk
            references "Users",
    "dayOfWeek"    varchar   not null,
    "weekList"     integer[] not null,
    "classStart"   smallint  not null,
    "classEnd"     smallint  not null,
    location       varchar   not null
);

alter table "courseSectionClasses"
    owner to checker;

create table "Students"
(
    id             serial,
    "userId"       integer not null
        constraint students_pk
            primary key
        constraint students_users_userid_fk
            references "Users"
            on delete cascade,
    "majorId"      integer not null
        constraint students_majors_id_fk
            references "Majors"
            on delete cascade,
    "firstName"    varchar not null,
    "lastName"     varchar not null,
    "enrolledDate" date    not null
);

alter table "Students"
    owner to checker;

create table "Instructors"
(
    id          serial
        constraint instructors_pk
            primary key,
    "userId"    integer not null
        constraint instructors_users_userid_fk
            references "Users"
            on delete cascade,
    "firstName" varchar not null,
    "lastName"  varchar not null
);

alter table "Instructors"
    owner to checker;

create table course_select
(
    "studentId" integer not null
        constraint course_select_students__fk
            references "Students"
            on delete cascade,
    "sectionId" integer not null
        constraint course_select_coursesections_id_fk
            references "courseSections"
            on delete cascade,
    "gradeType" varchar,
    grade       smallint,
    constraint course_select_pk
        primary key ("studentId", "sectionId")
);

alter table course_select
    owner to checker;

create table "majorCourses"
(
    id         serial
        constraint majorcourses_pk
            primary key,
    "majorId"  integer not null
        constraint majorcourses_majors_id_fk
            references "Majors"
            on delete cascade,
    "courseId" varchar not null
        constraint majorcourses_courses__fk
            references "Courses"
            on delete cascade,
    selection  varchar not null
);

alter table "majorCourses"
    owner to checker;

create function get_time_bad(sid integer, cid integer) returns character varying[]
    language plpgsql
as
$$
declare
    arr varchar[];
BEGIN
    arr = array_agg(fullname)
          from (select *
                from (select ca."courseName" || '[' || sn || ']' fullname
                      from (select val.s s, val.sn sn
                            from (SELECT cs.id i, cs."semesterId" sm, cs."courseId" s, cs."sectionName" sn, *
                                  from "courseSections" cs
                                  where cs.id = cid) val
                                     join (select "sectionId", ca."semesterId", "studentId"
                                           from course_select cr
                                                    join "courseSections" ca on cr."sectionId" = ca.id and cr."studentId" = sid) sub
                                          on sub."sectionId" = val.i and sub."semesterId" = val.sm) a
                               join "Courses" ca on ca."courseId" = a.s) p
                union
                (select *
                 from (select ful fullname
                       from (select cse."sectionId" l, *
                             from "courseSectionClasses" cse
                                      join "courseSections" cc on cse."sectionId" = cc.id) csc
                                join (select last_time.cna || '[' || "sectionName" || ']' ful,
                                             "dayOfWeek",
                                             "weekList",
                                             i,
                                             last_time."classStart",
                                             last_time."classEnd",
                                             grading,
                                             "sectionName",
                                             last_time."semesterId"
                                      from (select C."courseName" cna, *
                                            from (select *
                                                  from (select c_s.id i, *
                                                        from course_select cs
                                                                 join "courseSections" c_s
                                                                      on c_s.id = cs."sectionId" and cs."studentId" = (sid)) sections
                                                           join "courseSectionClasses" csc on csc."sectionId" = i) body
                                                     join "Courses" C on body."courseId" = C."courseId") last_time
                                               join "Semesters" s on last_time."semesterId" = s.id) t
                                     on ((csc."classEnd" >= t."classStart" and csc."classEnd" <= t."classEnd") or
                                         (csc."classStart" >= t."classEnd" and csc."classStart" <= t."classEnd")) and
                                        csc."semesterId" = t."semesterId" and csc."dayOfWeek" = t."dayOfWeek" and
                                        not check_week_fine(csc."weekList", t."weekList") and csc.l = (cid)) b
                 group by fullname
                 order by fullname)) c
          group by fullname
          order by fullname;
    return arr;
end
$$;

alter function get_time_bad(integer, integer) owner to checker;

create function check_place_fine(places character varying[], place character varying) returns boolean
    language plpgsql
as
$$
declare
    pla varchar;
begin
    foreach pla IN ARRAY places
        loop
            if place like '%' || pla || '%' then return true; end if;
        end loop;
    return false;
end;
$$;

alter function check_place_fine(character varying[], varchar) owner to checker;

create function check_week_fine(week_a integer[], week_b integer[]) returns boolean
    language plpgsql
as
$$
DECLARE
    wa integer; wb integer;
BEGIN
    foreach wa IN ARRAY week_a
        loop
            foreach wb IN ARRAY week_b
                loop
                    if wa = wb then return false; end if;
                end loop;
        end loop;
    return true;
END;
$$;

alter function check_week_fine(integer[], integer[]) owner to checker;

create function check_course_full(csc_id integer) returns boolean
    language plpgsql
as
$$
BEGIN
    return (select "leftCapacity" from "courseSections" where id = (csc_id)) = 0;
END;
$$;

alter function check_course_full(integer) owner to checker;

create function check_prerequisite(pre_id integer, stu integer) returns boolean
    language plpgsql
as
$$
DECLARE
    pre_type varchar; pres int[]; pre int;
BEGIN
    pre_type = (select type from prerequisite_list where prerequisite_list.id = pre_id);
    if pre_type = 'Atom' then
        return (select count(*)
                from (select cs.id
                      from "AtomPrerequisites" bp
                               join "courseSections" cs
                                    on bp."listId" = pre_id and bp."courseId" = cs."courseId") course_name
                         join course_select css
                              on css."sectionId" = course_name.id and css."studentId" = stu and css.grade >= 60) != 0;
    elseif pre_type = 'And' then
        pres = (select terms from "AndPrerequisites" where "listId" = pre_id);
        foreach pre IN ARRAY pres
            loop
                if not check_prerequisite(pre, stu) then return false; end if;
            end loop;
        return true;
    elseif pre_type = 'Or' then
        pres = (select terms from "OrPrerequisites" where "listId" = pre_id);
        foreach pre IN ARRAY pres
            loop
                if check_prerequisite(pre, stu) then return true; end if;
            end loop;
        return false;
    else
        RAISE EXCEPTION '====????====';
    end if;
END;
$$;

alter function check_prerequisite(integer, integer) owner to checker;

create function check_prerequisite_by_csc_id(csc_id integer, stu integer) returns boolean
    language plpgsql
as
$$
declare
    pre_id integer;
begin
    pre_id = (select root_prerequisite
              from "Courses" c
                       join "courseSections" cs on c."courseId" = cs."courseId" and cs.id = csc_id);
    if pre_id = 0 then return true; end if; return check_prerequisite(pre_id, stu);
end;
$$;

alter function check_prerequisite_by_csc_id(integer, integer) owner to checker;

create function check_course_passed(csc_id integer, stu integer) returns boolean
    language plpgsql
as
$$
BEGIN
    return
            (select count(*)
             from (SELECT ncs.id
                   from "courseSections" ncs
                   where id = csc_id) val
                      join course_select c
                           on c."sectionId" = val.id and c."studentId" = (stu) and coalesce(c.grade, -1) >= 60) != 0;
END;
$$;

alter function check_course_passed(integer, integer) owner to checker;

create function drop_course(sid integer, cid integer) returns boolean
    language plpgsql
as
$$
declare
    noGrade boolean;
begin
    noGrade = (select grade from course_select where "studentId" = sid and "sectionId" = cid) IS NULL;
    if noGrade then
        delete from course_select where "studentId" = sid and "sectionId" = cid;
        update "courseSections" set "leftCapacity" = "leftCapacity" + 1 where id = cid;
    end if;
    return noGrade;
end;
$$;

alter function drop_course(integer, integer) owner to checker;

create function get_all_course_and_grade(sid integer)
    returns TABLE
            (
                a character varying,
                b character varying,
                c integer,
                d integer,
                e character varying,
                f character varying,
                g integer
            )
    language plpgsql
as
$$
begin
    return query (select c."courseId",
                         c."courseName",
                         c.credit,
                         c."classHour",
                         c.grading,
                         coalesce("gradeType", 'null'),
                         coalesce(grade, -1)
                  from (select *
                        from course_select
                        where "studentId" = sid) c_s
                           join "courseSections" cS on cS.id = c_s."sectionId"
                           join "Courses" c on cS."courseId" = c."courseId");
end;
$$;

alter function get_all_course_and_grade(integer) owner to checker;

create function get_student_in_semester(courseid character varying, semester integer)
    returns TABLE
            (
                a integer,
                b character varying,
                c character varying,
                d date,
                e integer,
                f character varying,
                g integer,
                h character varying
            )
    language plpgsql
as
$$
begin
    return query (select "studentId",
                         "firstName",
                         "lastName",
                         "enrolledDate",
                         m.id,
                         m.name,
                         d.id,
                         d.name
                  from ((select "studentId"
                         from (select id
                               from "courseSections"
                               where "courseId" = courseId
                                 and "semesterId" = semester) cS
                                  join course_select c_s on "sectionId" = cS.id) stus
                      join "Students" s on stus."studentId" = s."userId") sub
                           join "Majors" m on m.id = sub."majorId"
                           join "Departments" D on m."departmentId" = D.id);
end;
$$;

alter function get_student_in_semester(varchar, integer) owner to checker;

create function get_course_table(sid integer, date date)
    returns TABLE
            (
                a character varying,
                b character varying,
                c integer,
                d character varying,
                e character varying,
                f integer,
                g integer,
                h character varying,
                i character varying,
                j integer[],
                k integer
            )
    language plpgsql
as
$$
declare
    semester int; begin_d int; week int;
begin
    semester = (select id from "Semesters" where begin_date <= date and end_date >= date);
    begin_d = (select begin_date from "Semesters" where id = semester);
    week = (date - begin_d) % 7 + 1;
    return query (
        select "courseName",
               "sectionName",
               "instructorId",
               "firstName",
               "lastName",
               "classStart",
               "classEnd",
               location,
               "dayOfWeek",
               "weekList",
               week
        from (select "courseName",
                     "sectionName",
                     "instructorId",
                     "firstName",
                     "lastName",
                     "classStart",
                     "classEnd",
                     location,
                     "dayOfWeek",
                     "weekList"
              from (select "sectionId" from course_select where "studentId" = sid) c_S
                       join "courseSections" cS on cS.id = c_S."sectionId"
                  and "semesterId" = semester
                       join "Courses" C2 on cS."courseId" = C2."courseId"
                       join "courseSectionClasses" csc on c_S."sectionId" = cS.id
                       join "Instructors" ins on ins."userId" = csc."instructorId") p);

end;
$$;

alter function get_course_table(integer, date) owner to checker;

create function add_course_with_grade(sid integer, cid integer, gtype character varying, score smallint) returns void
    language plpgsql
as
$$
declare
    type varchar;
begin
    type = (select grading
            from (select "courseId" from "courseSections" cS where cS.id = cid) t
                     join "Courses" c on t."courseId" = c."courseId");
    if gType is null
    then
        insert into course_select values (sid, cid, gType, score);
    else
        if (gType = type)
        then
            insert into course_select values (sid, cid, gType, score);
        end if;
    end if;
end
$$;

alter function add_course_with_grade(integer, integer, varchar, smallint) owner to checker;

create function get_course_and_grade_in_semester(student integer, semester integer)
    returns TABLE
            (
                a character varying,
                b character varying,
                c integer,
                d integer,
                e character varying,
                f character varying,
                g integer
            )
    language plpgsql
as
$$
begin
    return query (select c."courseId",
                         c."courseName",
                         c.credit,
                         c."classHour",
                         c.grading,
                         coalesce("gradeType", 'null') gt,
                         coalesce(grade, -1)           g
                  from (select *
                        from course_select
                        where "studentId" = student) c_s
                           join "courseSections" cS on cS.id = c_s."sectionId" and "semesterId" = semester
                           join "Courses" c on cS."courseId" = c."courseId");
end;
$$;

alter function get_course_and_grade_in_semester(integer, integer) owner to checker;

create function search_course(student_id integer, semester_id integer, search_cid character varying,
                              search_name character varying, search_instructor character varying,
                              search_dayofweek character varying, search_classtime smallint,
                              searchclasslocations character varying[], searchcoursetype character varying,
                              ignorefull boolean, ignoreconflict boolean, ignorepassed boolean,
                              ignoremissingprerequisites boolean, pagesize integer, pageindex integer)
    returns TABLE
            (
                a character varying,
                b character varying,
                c integer,
                d integer,
                e character varying,
                f integer,
                g character varying,
                h integer,
                i integer,
                j character varying[],
                k integer,
                l integer,
                m character varying
            )
    language plpgsql
as
$$
begin
    return query (select cname,
                         fir.nme,
                         fir.credit,
                         fir."classHour",
                         fir.grading,
                         csid,
                         fir."sectionName",
                         fir."totalCapacity",
                         fir."leftCapacity",
                         get_time_bad(student_id, csid) ct,
                         fir."semesterId",
                         fir.root_prerequisite,
                         selection
                  from (select mc."majorId" mid, *
                        from (select *
                              from (select cs.id csid, cs."courseId" cname, c."courseName" nme, *
                                    from "courseSections" cs
                                             join "Courses" c on c."courseId" = cs."courseId") fi
                                       join "courseSectionClasses" csc on csc."sectionId" = fi.csid) f
                                 left join "majorCourses" mc on mc."courseId" = f.cname) fir
                           join "Users" u on fir."instructorId" = u."userId" and "semesterId" = semester_id and
                                             not (ignoreFull and check_course_full(fir.csid)) and
                                             not (ignoreConflict and get_time_bad((student_id), csid) is not null) and
                                             not (ignorePassed and check_course_passed(fir.csid, student_id)) and
                                             not (ignoreMissingPrerequisites and
                                                  not check_prerequisite_by_csc_id(fir.csid, student_id)) and
                                             (search_cid is null or fir.cname like '%' || search_cid || '%') and
                                             (search_name is null or
                                              fir.nme || '[' || fir."sectionName" || ']' like
                                              '%' || search_name || '%') and
                                             (search_classtime is null or
                                              ("classStart" <= search_classtime and "classEnd" >= search_classtime))
                      and
                                             (searchCourseType is null or
                                              ((searchCourseType != 'MAJOR_COMPULSORY' or (selection = 'COMPULSORY')) and
                                               (searchCourseType != 'MAJOR_ELECTIVE' or (selection = 'ELECTIVE')) and
                                               (searchCourseType != 'PUBLIC' or (selection is null)) and
                                               (searchCourseType != 'CROSS_MAJOR' or (selection is not null)) and
                                               (searchCourseType = 'ALL' or searchCourseType = 'PUBLIC' or
                                                ((searchCourseType = 'CROSS_MAJOR' and (select count(*)
                                                                                        from "Students" ss
                                                                                        where ss."userId" = student_id
                                                                                          and ss."majorId"
                                                                                            = mid) =
                                                                                       0) or
                                                 (searchCourseType != 'CROSS_MAJOR' and (select count(*)
                                                                                         from "Students" ss
                                                                                         where ss."userId" = student_id
                                                                                           and ss."majorId" != mid) =
                                                                                        0)))))
                      and
                                             (search_dayofweek is null or "dayOfWeek" = search_dayofweek) and
                                             (search_instructor is null or
                                              "firstName" like (search_instructor || '%') or
                                              "lastName" like (search_instructor || '%') or
                                              "firstName" || "lastName" like (search_instructor || '%')) and
                                             (searchClassLocations is null or
                                              check_place_fine(searchClassLocations, fir.location))
                  group by (csid, cname, fir.nme, fir."sectionName", fir."totalCapacity",
                            fir."leftCapacity", fir.credit, fir."classHour", fir.grading, fir."semesterId",
                            fir.root_prerequisite, selection)
                  order by cname, fir.nme || '[' || fir."sectionName" || ']'
                  offset pageSize * pageIndex limit pageSize);
end
$$;

alter function search_course(integer, integer, varchar, varchar, varchar, varchar, smallint, character varying[], varchar, boolean, boolean, boolean, boolean, integer, integer) owner to checker;


