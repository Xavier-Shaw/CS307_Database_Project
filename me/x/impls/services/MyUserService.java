package me.x.impls.services;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.Instructor;
import cn.edu.sustech.cs307.dto.User;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.service.UserService;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MyUserService implements UserService {

    @Override
    public void removeUser(int userId) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("delete from Users where userId = (?)");
             PreparedStatement second_query = connection.prepareStatement("")
        ) {
            first_query.setInt(1,userId);
            first_query.execute();
            //TODO: delete the related data
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }

    @Override
    public List<User> getAllUsers() {
        ArrayList<User> users = new ArrayList<>();
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("select * from Users");
        ) {
            ResultSet resultSet = first_query.executeQuery();
            boolean empty = true;
            while (resultSet.next()){
                empty = false;
                User user = new Instructor();
                user.id = resultSet.getInt(1);
                String firstName = resultSet.getString(2);
                String lastName = resultSet.getString(3);
                byte judge = (byte) firstName.charAt(0);
                if ( (judge >= 65 && judge <= 90) || (judge >= 97 && judge <= 122)) {
                    user.fullName = firstName + " " + lastName;
                }
                else {
                    user.fullName = firstName + lastName;
                }
                users.add(user);
            }

            if (empty){
                throw new EntityNotFoundException();
            }
            else {
                return users;
            }
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }

    @Override
    public User getUser(int userId) {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement first_query = connection.prepareStatement("select * from Users where id = (?)");
        ) {
            first_query.setInt(1,userId);
            ResultSet resultSet = first_query.executeQuery();
            if (resultSet.next()){
                User user = new Instructor();
                user.id = resultSet.getInt(1);
                String firstName = resultSet.getString(2);
                String lastName = resultSet.getString(3);
                byte judge = (byte) firstName.charAt(0);
                if ( (judge >= 65 && judge <= 90) || (judge >= 97 && judge <= 122)) {
                    user.fullName = firstName + " " + lastName;
                }
                else {
                    user.fullName = firstName + lastName;
                }
                return user;
            }
            else {
                throw new EntityNotFoundException();
            }
        } catch (SQLException e) {
            throw new EntityNotFoundException();
        }
    }
}
