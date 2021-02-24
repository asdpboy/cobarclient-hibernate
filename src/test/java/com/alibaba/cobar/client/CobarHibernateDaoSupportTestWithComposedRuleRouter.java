package com.alibaba.cobar.client;


import java.util.ArrayList;
import java.util.List;

import com.alibaba.cobar.client.entities.User;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


@Test(sequential = true)
public class CobarHibernateDaoSupportTestWithComposedRuleRouter extends
        AbstractTestNGCobarClientTest {

    private CobarHibernateDaoSupport dao        = new CobarHibernateDaoSupport();
    private int[]                      userIds  = new int[] { 1, 2, 3, 4, 5, 6, 7};

    public static final String          CREATE_SQL = "com.alibaba.cobar.client.entities.User.save";
    public static final String          UPDATE_SQL = "com.alibaba.cobar.client.entities.User.update";
    public static final String          DELETE_SQL = "com.alibaba.cobar.client.entities.User.delete";

    public CobarHibernateDaoSupportTestWithComposedRuleRouter() {
        super(new String[] { "META-INF/spring/cobar-client-appctx.xml",
                "META-INF/spring/datasources-appctx.xml",
                "META-INF/spring/classname-methodname-composed-router-appctx.xml" });
    }

    @BeforeTest
    public void setupDaoSupport() {
        dao.setHibernateTemplate(getHibernateTemplate());
    }

    public void testBatchInsertOnDaoSupport() {
        verifyNonExistenceOnPartitions();
        List<User> users = createUsersWithUserIds(userIds);
        dao.getHibernateTemplate().saveOrUpdateAll(users);
        verifyExistenceOnPartitions();
    }

    public void testBatchUpdateOnDaoSupport() {
        verifyNonExistenceOnPartitions();
        List<User> users = createUsersWithUserIds(userIds);
        
        for (User User : users) {
            User.setUsername("_username_to_update_");
        }
        dao.getHibernateTemplate().saveOrUpdateAll(users);

    }

//    public void testBatchDeleteOnDaoSupport() {
//        verifyNonExistenceOnPartitions();
//        List<User> users = createUsersWithUserIds(userIds);
//        dao.getHibernateTemplate().deleteAll(users);
//    }

    private List<User> createUsersWithUserIds(int[] userIds) {
        List<User> Users = new ArrayList<User>();
        for (int id : userIds) {
            User User = new User();
            User.setId(id);
            User.setUsername("username" + id );
            User.setPassword("password" + id);
            Users.add(User);
        }
        return Users;
    }

    private void verifyNonExistenceOnPartitions() {
        for (int i = 0; i < userIds.length; i++) {
            String confirmSQL = "select username from User where id=" + userIds[i];
            if (userIds[i] % 2 == 0) {
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
            } else {
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
            }
        }
    }

    private void verifyExistenceOnPartitions() {
        for (int i = 0; i < userIds.length; i++) {
            String confirmSQL = "select username from user where id=" + userIds[i];
            if (userIds[i] % 2 == 0) {
                verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
            } else {
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
                verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
            }
        }
    }
}
