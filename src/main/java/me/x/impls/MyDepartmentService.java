package me.x.impls.services;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.Department;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.DepartmentService;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MyDepartmentService implements DepartmentService {


    /**
     *  if adding a new department which has the same name with an existing department,
     *  it should throw an {@code IntegrityViolationException}
     * @param name
     * @return
     */
    @Override
    public int addDepartment(String name) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("insert into Departments (name) values (?)");
             PreparedStatement second_query = connection.prepareStatement("select id from Departments where name = (?)")
        ) {
            first_query.setString(1,name);
            second_query.setString(1,name);
            first_query.execute();
            ResultSet resultSet = second_query.executeQuery();
            resultSet.next();
            return resultSet.getInt(1);
        } catch (SQLException e) {
            throw new IntegrityViolationException();
        }
    }

    @Override
    public void removeDepartment(int departmentId) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("delete from Departments where id = (?)");
        ) {
            first_query.setInt(1,departmentId);
            first_query.execute();
            //TODO: 删除与该department有关的major ?
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }

    @Override
    public List<Department> getAllDepartments() {
        ArrayList<Department> departments = new ArrayList<>();
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("select * from Departments");
        ) {
            ResultSet resultSet = first_query.executeQuery();
            while (resultSet.next()){
                Department department = new Department();
                department.id = resultSet.getInt(1);
                department.name = resultSet.getString(2);
                departments.add(department);
            }
            return departments;
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }


    /**
     * If there is no Department about specific id, throw EntityNotFoundException.
     * @param departmentId
     * @return
     */
    @Override
    public Department getDepartment(int departmentId) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("select * from Departments where id = (?)");
        ) {
            first_query.setInt(1,departmentId);
            ResultSet resultSet = first_query.executeQuery();
            boolean empty = true;
            Department department = new Department();
            department.id = departmentId;
            while (resultSet.next()){
                empty = false;
                department.name = resultSet.getString(2);
            }
            if (empty){
                throw new EntityNotFoundException();
            }
            else {
                return department;
            }
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }
}
