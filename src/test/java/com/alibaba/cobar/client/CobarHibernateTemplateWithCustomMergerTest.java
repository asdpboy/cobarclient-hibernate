package com.alibaba.cobar.client;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.alibaba.cobar.client.entities.User;
import org.springframework.orm.hibernate3.HibernateTemplate;
import org.testng.annotations.Test;

import com.alibaba.cobar.client.support.utils.CollectionUtils;

public class CobarHibernateTemplateWithCustomMergerTest extends AbstractTestNGCobarClientTest {

    public CobarHibernateTemplateWithCustomMergerTest() {
        super(new String[] { "META-INF/spring/datasources-appctx.xml",
                "META-INF/spring/cobar-client-appctx.xml",
                "META-INF/spring/cobar-client-custom-merger-appctx.xml" });
    }

    @Test
    public void testQueryForListWithCustomMerger() {
        batchInsertUsersAsFixture();

        HibernateTemplate st = (HibernateTemplate) getApplicationContext().getBean(
                "hibernateTemplateWithMerger");
        @SuppressWarnings("unchecked")
        List lst = st.find("from User order by password");
        
        System.out.println("Orgin List : " + lst);
        
        assertTrue(CollectionUtils.isNotEmpty(lst));
        
        assertEquals(5, lst.size());

        verifyUsersOrderBySubject(lst);

    }

    @SuppressWarnings("unchecked")
    private void verifyUsersOrderBySubject(List lst) {
    	System.out.println(lst);
        for (int i = 0; i < lst.size(); i++) {
            User user = (User) lst.get(i);
            if (i == 0 || i == 1) {
                assertEquals(2, Integer.parseInt(user.getUsername()));
            } else {
                assertEquals(1, Integer.parseInt(user.getUsername()));
            }
            switch (i) {
                case 0:
                    assertEquals("A", user.getPassword());
                    break;
                case 1:
                    assertEquals("D", user.getPassword());
                    break;
                case 2:
                    assertEquals("S", user.getPassword());
                    break;
                case 3:
                    assertEquals("X", user.getPassword());
                    break;
                case 4:
                    assertEquals("Z", user.getPassword());
                    break;
                default:
                    throw new IllegalArgumentException("unexpected condition.");
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testQueryForListWithoutCustomMerger() {
        batchInsertUsersAsFixture();

        List lst = getHibernateTemplate().find("from User order by password");

        assertTrue(CollectionUtils.isNotEmpty(lst));
        // contains all of the entities, but the order is not guaranteed.
        assertEquals(5, lst.size());
        
        // sort in application code
        Comparator<User> comparator = (Comparator<User>) getApplicationContext().getBean(
                "comparator");
        Collections.sort(lst, comparator);
        verifyUsersOrderBySubject(lst);
    }

    private void batchInsertUsersAsFixture() {
        List<User> users = new ArrayList<User>();
        
        User user = new User();
        user.setId(1);
        user.setUsername("1");
        user.setPassword("Z");
        users.add(user);

        user = new User();
        user.setId(2);
        user.setUsername("1");
        user.setPassword("X");
        users.add(user);

        user = new User();
        user.setId(3);
        user.setUsername("1");
        user.setPassword("S");
        users.add(user);

        user = new User();
        user.setId(4);
        user.setUsername("2");
        user.setPassword("D");
        users.add(user);

        user = new User();
        user.setId(5);
        user.setUsername("2");
        user.setPassword("A");
        users.add(user);
        
        getHibernateTemplate().saveOrUpdateAll(users);
    }
}
