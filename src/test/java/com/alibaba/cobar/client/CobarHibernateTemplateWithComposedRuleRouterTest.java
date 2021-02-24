package com.alibaba.cobar.client;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.cobar.client.entities.User;
import org.apache.commons.lang.ArrayUtils;
import org.testng.annotations.Test;

import com.alibaba.cobar.client.support.utils.CollectionUtils;

@Test(sequential=true)
public class CobarHibernateTemplateWithComposedRuleRouterTest extends
        AbstractTestNGCobarClientTest {

    public CobarHibernateTemplateWithComposedRuleRouterTest() {
        super(new String[] { "META-INF/spring/cobar-client-appctx.xml",
                "META-INF/spring/datasources-appctx.xml",
                "META-INF/spring/classname-methodname-composed-router-appctx.xml" });
    }

    public void testInsertOnCobarHibernateTemplate() {
        User u = new User();
        u.setId(1);
        u.setUsername("username1");
        Object pk = getHibernateTemplate().save(u);
        assertNotNull(pk);

        String confirmSQL = "SELECT id FROM user where id=" + pk;
        verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);

        User u1 = new User();
        u1.setId(2);
        u1.setUsername("username2");
        pk = null;
        pk = getHibernateTemplate().save(u1);
        assertNotNull(pk);
        
        confirmSQL = "SELECT id FROM user where id=" + pk;
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
    }

    public void testInsertOnCobarHibernateTemplateWithoutFoundRule() {
    	User u1 = new User();
        u1.setId(3);
        u1.setUsername("username2");
        Object pk = getHibernateTemplate().save(u1);
        assertNotNull(pk);

        String confirmSQL = "SELECT id FROM user where id=" + pk;
        
        verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
    }

    public void testBatchInsertOnCobarClientTemplateNormally() {
    	int[] ids = new int[] { 1, 2, 3, 4, 5, 6, 7 };
        batchInsertUsersAsFixtureForLaterUse(ids);

        for (int i = 0; i < ids.length; i++) {
            String confirmSQL = "select id from user where id=" + ids[i];
            if (ids[i] % 2 == 1) {
                verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
            } else {
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
                verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
            }
        }
        
        List<User> list = new ArrayList<User>() ;
        for (int id : ids) {
            User user = new User();
            user.setId(id);
            list.add(user) ;
        }
        
        try {
        	getHibernateTemplate().deleteAll(list);
		} catch (Exception e) {
			fail("delete error") ;
		}
    }

    public void testDeleteOnCobarHibernateTemplateNormally() {
        int[] ids = new int[] { 1, 2, 3, 4, 5, 6, 7 };
        // 1. empty data bases
        for (int mid : ids) {
            User user = new User();
            user.setId(mid);
            getHibernateTemplate().delete(user);
        }
        // 2. insert data fixtures
        batchInsertUsersAsFixtureForLaterUse(ids);
        for (int i = 0; i < ids.length; i++) {
            String confirmSQL = "select id from user where id=" + ids[i];
            if (ids[i] % 2 == 1) {
                verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
            } else {
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
                verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
            }
        }
        // 3. perform deletion and assertion
        List<User> list = new ArrayList<User>() ;
        for (int id : ids) {
            User user = new User();
            user.setId(id);
            list.add(user) ;
        }
        
        try {
        	getHibernateTemplate().deleteAll(list);
		} catch (Exception e) {
			fail("delete error") ;
		}
        
    }

    public void testDeleteOnCobarHibernateTemplateAbnormally() {
        int[] ids = new int[] { 1, 2, 3, 4, 5, 6, 7 };
        // 1. empty data bases
        for (int mid : ids) {
            User user = new User();
            user.setId(mid);
            getHibernateTemplate().delete(user);
        }
        
        batchInsertUsersAsFixtureForLaterUse(ids);

        String deleteSqlAction = "com.alibaba.cobar.client.entities.User.delete";
        // no rule can be found for this, so currently, it will be performed against default data source
        for (int i = 0; i < ids.length; i++) {
            String selectSQL = "select id from users where memberId=" + ids[i];
            if (ids[i] % 2 == 1 ) {
                Long id = jt1m.queryForLong(selectSQL);
                try {
                	getHibernateTemplate().delete(new User(ids[i]));
				} catch (Exception e) {
					fail("delete error") ;
				}
                
            } else {
                Long id = jt2m.queryForLong(selectSQL);
                try {
                	getHibernateTemplate().delete(new User(ids[i]));
				} catch (Exception e) {
					fail("delete error") ;
				}
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testQueryForListOnCobarHibernateTemplateNormally() {
    	int[] ids = new int[] { 1, 2, 3, 4, 5, 6, 7 };
        // 1. empty data bases
        for (int mid : ids) {
            User user = new User();
            user.setId(mid);
            getHibernateTemplate().delete(user);
        }
        batchInsertUsersAsFixtureForLaterUse(ids);

        List<User> users = (List<User>) getHibernateTemplate().find("from User");
        assertTrue(CollectionUtils.isNotEmpty(users));
        
        assertEquals(7, users.size());
        for (User user : users) {
            assertTrue(ArrayUtils.contains(ids, user.getId()));
        }

        //users = null;
        //users = (List<User>) getHibernateTemplate().queryForList("com.alibaba.cobar.client.entities.User.findByMemberIdRange", 300L);
        //assertTrue(CollectionUtils.isNotEmpty(users));
        //assertEquals(3, users.size());
        //Long[] partialMemberIds = new Long[] { 1L, 129L, 257L };
        //for (User user : users) {
        //    assertTrue(ArrayUtils.contains(partialMemberIds, user.getMemberId()));
        //}
    }

    public void testQueryForObjectOnCobarHibernateTemplateNormally() {
    	int[] ids = new int[] { 1, 2, 3, 4, 5, 6, 7 };
        // 1. empty data bases
        for (int mid : ids) {
            User user = new User();
            user.setId(mid);
            getHibernateTemplate().delete(user);
        }
        batchInsertUsersAsFixtureForLaterUse(ids);

        // scenario 1: no routing rules are found for current sql action, so only records residing on default data source can be returned.
        for (int i = 0; i < ids.length; i++) {
            String confirmSQL = "select id from user where id=" + ids[i];
            if (i < 3) {
                Long id = jt1m.queryForLong(confirmSQL);
                User user = (User) getHibernateTemplate().get(User.class, id);
                assertNotNull(user);
                assertEquals(ids[i], user.getId());
            } else {
                Long id = jt2m.queryForLong(confirmSQL);
                User user = (User) getHibernateTemplate().get(User.class, id);
                assertNull(user);
            }
        }
        // scenario 2: fallback sharding rules can be found for current sql action, so all requested records are returned normally.
        for (int id : ids) {
            User user = (User) getHibernateTemplate().load(User.class, id);
            assertNotNull(user);
            assertEquals(id, user.getId());
        }
    }

    public void testUpdateOnCobarHibernateTemplateNormally() {
    	int[] ids = new int[] { 1, 2, 3, 4, 5, 6, 7 };
        // 1. empty data bases
        for (int mid : ids) {
            User user = new User();
            user.setId(mid);
            getHibernateTemplate().delete(user);
        }
        batchInsertUsersAsFixtureForLaterUse(ids);

        String userSubject = "_SUBEJCT_";
        for (int id : ids) {
            User user = (User) getHibernateTemplate().get(User.class, id);
            assertNotNull(user);
            assertEquals("username" + id, user.getUsername());
            // 2. assertion on update
            user.setUsername(userSubject);
            getHibernateTemplate().update(user);
            // 3. assertion after update
            user = null;
            user = (User) getHibernateTemplate().get(User.class, id);
            assertNotNull(user);
            assertEquals(userSubject, user.getUsername());
        }

    }

    private void batchInsertUsersAsFixtureForLaterUse(int[] ids) {
        List<User> users = new ArrayList<User>();
        for (int id : ids) {
            User user = new User();
            user.setUsername("username" + id );
            user.setId(id);
            users.add(user);
        }

        getHibernateTemplate().saveOrUpdateAll(users);
    }

}
