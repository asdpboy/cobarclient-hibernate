package com.alibaba.cobar.client;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.cobar.client.entities.Depart;
import com.alibaba.cobar.client.entities.Role;
import org.apache.commons.lang.ArrayUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.JdbcUpdateAffectedIncorrectNumberOfRowsException;
import org.testng.annotations.Test;

import com.alibaba.cobar.client.support.HQLBean;
import com.alibaba.cobar.client.support.utils.CollectionUtils;

@Test(sequential=true)
public class CobarHibernateTemplateWithClassNameRouterTest extends AbstractTestNGCobarClientTest {

    public CobarHibernateTemplateWithClassNameRouterTest() {
        super(new String[] { "META-INF/spring/cobar-client-appctx.xml",
                "META-INF/spring/datasources-appctx.xml",
                "META-INF/spring/classname-router-appctx.xml" });
    }

    public void testInsertOnCobarHibernateTemplate() {
    	Role r = new Role();
    	r.setId(1);
    	r.setUsername("tcontent");
    	getHibernateTemplate().save(r) ;
    	
    	try {
    		System.out.println(((CobarHibernateTemplate)getHibernateTemplate()).getParameters("delete  FROM Role where id=:id", null));
    		HQLBean hqlBean = ((CobarHibernateTemplate)getHibernateTemplate()).getHQLBeanByHql("update Role r set r.id=:id");
    		ArrayList list = new ArrayList() ;
    		list.add("role0_.id=:id") ;
    		
    		Map map = new HashMap() ;
    		map.put("id", 20) ;
    		
			System.out.println(((CobarHibernateTemplate)getHibernateTemplate()).hqlBean2Entity(hqlBean, list, map , true, true));
    		System.out.println(getHibernateTemplate().getClass().getMethod("get" , Class.class , Serializable.class).invoke(getHibernateTemplate(), Role.class , 1));
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        String confirmSQL = "select username from Role where username='tcontent'";

        verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);

        Depart d = new Depart();
		d.setId(1);
		d.setUsername("fname");
        getHibernateTemplate().save(d ) ;
        

        confirmSQL = "select username from Depart where username='fname'";
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
        getHibernateTemplate().delete(d);
    }

    public void testInsertInBatchOnCobarHibernateTemplate() {
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };
        batchInsertMultipleDepartsAsFixture(names);

        for (String name : names) {
            String sql = "select username from Depart where username='" + name + "'";
            verifyEntityNonExistenceOnSpecificDataSource(sql, jt1m);
            verifyEntityNonExistenceOnSpecificDataSource(sql, jt1s);
            verifyEntityExistenceOnSpecificDataSource(sql, jt2m);
            verifyEntityNonExistenceOnSpecificDataSource(sql, jt2s);
        }
        
        
    }
    

    protected void batchInsertMultipleDepartsAsFixture(String[] names) {
        List<Depart> ds = new ArrayList<Depart>();
        int i = 3 ;
        for (String name : names) {
        	Depart d  = new Depart() ;
        	d.setId(i);
        	d.setUsername(name);
            ds.add(d);
            i ++ ;
        }

        getHibernateTemplate().saveOrUpdateAll(ds);
    }

    public void testDeleteOnCobarSqlMapClientTemplate() {
    	Depart d = new Depart();
    	d.setId(2);
		d.setUsername("fname");
        //getSqlMapClientTemplate().insert("com.alibaba.cobar.client.entities.Depart.create", f);
    	getHibernateTemplate().save(d) ;
    	
        String confirmSQL = "select username from Depart where username='fname'";
        
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);

        getHibernateTemplate().delete(d);
        
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
    }

    public void testDeleteWithExpectedResultSizeOnCobarHibernateTemplate() {
    	Depart d = new Depart();
    	d.setId(2);
		d.setUsername("fname");
		getHibernateTemplate().save(d) ;
		
		String confirmSQL = "select username from Depart where username='fname'";
		
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);

        try {
        	getHibernateTemplate().delete(d);
            fail("only one row will be affected in fact.");
        } catch (DataAccessException e) {
            assertTrue(e instanceof JdbcUpdateAffectedIncorrectNumberOfRowsException);
            JdbcUpdateAffectedIncorrectNumberOfRowsException ex = (JdbcUpdateAffectedIncorrectNumberOfRowsException) e;
            assertEquals(1, ex.getActualRowsAffected());
        }
        // although JdbcUpdateAffectedIncorrectNumberOfRowsException is raised, but the delete does performed successfully.
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);

        try {
        	getHibernateTemplate().delete(d);
        } catch (DataAccessException e) {
            fail();
        }
    }

    public void testQueryForListOnCobarHibernateTemplate() {
        // 1. initialize data
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };
        batchInsertMultipleDepartsAsFixture(names);

        // 2. perform assertion
        @SuppressWarnings("unchecked")
        List<Depart> resultList = (List<Depart>) getHibernateTemplate().find("from Depart" , new Object[]{});
        assertTrue(CollectionUtils.isNotEmpty(resultList));
        assertEquals(5, resultList.size());
        for (Depart f : resultList) {
            assertTrue(ArrayUtils.contains(names, f.getUsername()));
        }

        // 3. perform assertion with another different query
        @SuppressWarnings("unchecked")
        List<Depart> followersWithNameStartsWithA = (List<Depart>) getHibernateTemplate().find("from Depart where username like ?", new String[]{"A%"});
        assertTrue(CollectionUtils.isNotEmpty(followersWithNameStartsWithA));
        assertEquals(3, followersWithNameStartsWithA.size());
        for (Depart f : followersWithNameStartsWithA) {
            assertTrue(ArrayUtils.contains(names, f.getUsername()));
        }
    }

    public void testQueryForMapOnCobarSqlMapClientTemplate() {
        // TODO low priority
    }

    /**
     * although we use queryForObject by querying on name column, but it doesn't
     * mean 'name' column is unique, it's because of the data fixture we set up
     * to use.
     */
    public void testQueryForObjectOnCobarSqlMapClientTemplate() {
        // 1. initialize data
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };
        batchInsertMultipleDepartsAsFixture(names);

        // 2. assertion.
        for (String name : names) {
            List<Depart> list  = getHibernateTemplate().find("from Depart where username=?", new String[]{name});
            Depart d = list.isEmpty() ? null : list.get(0) ;
            assertNotNull(d);
        }
    }

    public void testQueryWithRowHandlerOnHibernateTemplate() {
        // TODO low priority
    }

    /**
     * WARNING: don't do stupid things such like below, we do this because we
     * can guarantee the shard id will NOT change. if you want to use cobar
     * client corretly, make sure you are partitioning you databases with shard
     * id that will not be changed once it's created!!!
     */
    public void testUpdateOnCobarHibernateTemplate() {
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };
        batchInsertMultipleDepartsAsFixture(names);

        String nameSuffix = "Wang";
        for (String name : names) {
            List<Depart> list  = getHibernateTemplate().find("from Depart where username=?", new String[]{name});
            Depart d = list.isEmpty() ? null : list.get(0) ;
            assertNotNull(d);
            d.setUsername(d.getUsername() + nameSuffix);
            
            getHibernateTemplate().update(d);

            int id = d.getId();

            d = null;
            list.clear(); 
            
            list  = getHibernateTemplate().find("from Depart where username=?", new String[]{name});
            d = list.isEmpty() ? null : list.get(0) ;
            assertNull(d);
            d = (Depart) getHibernateTemplate().get(Depart.class, id);
            assertNotNull(d);
            assertEquals(name + nameSuffix, d.getUsername());
        }

    }

}
