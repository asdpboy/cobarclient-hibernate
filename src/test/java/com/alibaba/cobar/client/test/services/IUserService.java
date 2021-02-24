package com.alibaba.cobar.client.test.services;

import java.util.List;

import com.alibaba.cobar.client.entities.User;

public interface IUserService {
    void createUsersInBatch(List<User> users);
}
