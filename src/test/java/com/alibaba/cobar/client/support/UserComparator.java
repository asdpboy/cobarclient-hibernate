package com.alibaba.cobar.client.support;

import java.util.Comparator;

import com.alibaba.cobar.client.entities.User;


public class UserComparator implements Comparator<User> {

    public int compare(User user1, User user2) {
        return user1.getPassword().compareTo(user2.getPassword());
    }
    
    public static void main(String[] args) {
		User user1 = new User() ;
		user1.setPassword("D");
		
		User user2 = new User() ;
		user2.setPassword("E"); 
		
		System.out.println(user1.getPassword().compareTo(user2.getPassword()));
		
	}
}
