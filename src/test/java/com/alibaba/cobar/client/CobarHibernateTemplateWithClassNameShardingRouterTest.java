package com.alibaba.cobar.client;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.cobar.client.entities.Depart;
import org.apache.commons.lang.ArrayUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.JdbcUpdateAffectedIncorrectNumberOfRowsException;
import org.springframework.jdbc.core.RowMapper;
import org.testng.annotations.Test;

import com.alibaba.cobar.client.support.utils.CollectionUtils;

@Test(sequential=true)
public class CobarHibernateTemplateWithClassNameShardingRouterTest extends
        AbstractTestNGCobarClientTest {

    public CobarHibernateTemplateWithClassNameShardingRouterTest() {
        super(new String[] { "META-INF/spring/cobar-client-appctx.xml",
                "META-INF/spring/datasources-appctx.xml",
                "META-INF/spring/classname-sharding-router-appctx.xml" });
    }

    public void testInsertOnCobarHibernateTemplateWithSingleEntityNormally() {
        String name = "Darren"; // shard standard
        Depart d = new Depart();
        d.setId(1);
        d.setUsername(name);
        
        getHibernateTemplate().save(d);

        String sql = "select username from Depart where username='" + name + "'";
        
        verifyEntityNonExistenceOnSpecificDataSource(sql, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(sql, jt1s);
        verifyEntityExistenceOnSpecificDataSource(sql, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(sql, jt2s);

        name = "Aaron";
        d = new Depart();
        d.setId(2);
        d.setUsername(name);
        getHibernateTemplate().save(d);

        sql = "select username from Depart where username='" + name + "'";
        verifyEntityExistenceOnSpecificDataSource(sql, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(sql, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(sql, jt1s);
        verifyEntityNonExistenceOnSpecificDataSource(sql, jt2s);
    }

    public void testInsertOnCobarHibernateTemplateWithMultipleEntities() {
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };

        List<Depart> departs = new ArrayList<Depart>();
        int i = 5 ;
        for (String name : names) {
        	Depart d = new Depart() ;
        	d.setId(i);
        	d.setUsername(name);
        	departs.add(d);
        	i++ ;
        }
        /**
         * NOTE: if the sqlmap is drafted with invalid format, data access
         * exception will be raised, usually, the information of exception
         * doesn't tell too much.
         */
        getHibernateTemplate().saveOrUpdateAll(departs);

        for (String name : names) {
            String sql = "select username from Depart where username='" + name + "'";
            if (name.startsWith("A")) {
                verifyEntityExistenceOnSpecificDataSource(sql, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(sql, jt1s);
                verifyEntityNonExistenceOnSpecificDataSource(sql, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(sql, jt2s);
            } else {
                verifyEntityNonExistenceOnSpecificDataSource(sql, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(sql, jt1s);
                verifyEntityExistenceOnSpecificDataSource(sql, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(sql, jt2s);
            }

        }
    }

    /**
     * if no rule is found for current data access request, the data access
     * request will be performed on default data source, that's, partition1.
     */
    public void testInsertWithoutFindingRuleOnCobarHibernateTemplate() {
        String nameStartsWithS = "Sara";
        Depart d = new Depart();
        d.setId(3);
        d.setUsername(nameStartsWithS);
        
        getHibernateTemplate().save(d);

        String confirmSQL = "select username from Depart where username='" + nameStartsWithS + "'";
        verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
    }

    public void testDeleteOnCobarHibernateTemplate() {
    	Depart d = new Depart();
    	d.setId(4);
    	d.setUsername("Darren");
    	
        getHibernateTemplate().save(d);

        String confirmSQL = "select username from depart where username='Darren'";
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

    /**
     * MARK!!!
     */
    public void testDeleteWithExpectedResultSizeOnCobarHibernateTemplate() {
    	Depart d = new Depart();
    	d.setId(4);
    	d.setUsername("Darren");
    	
        getHibernateTemplate().save(d);

        String confirmSQL = "select username from Depart where username='Darren'";
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

        d = new Depart(4, "Amanda");
        try {
            getHibernateTemplate().delete(d);
        } catch (DataAccessException e) {
            fail();
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

    public void testQueryForListOnCobarHibernateTemplate() {
        // 1. initialize data
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };
        batchInsertMultipleDepartsAsFixture(names);

        // 2. perform assertion
        @SuppressWarnings("unchecked")
        List<Depart> resultList = (List<Depart>) getHibernateTemplate().find("from Depart" , new String[]{});
        
        assertTrue(CollectionUtils.isNotEmpty(resultList));
        assertEquals(5, resultList.size()); // no rule match 'findAll', so query is performed against default data source - partition1
        for (Depart f : resultList) {
            assertTrue(ArrayUtils.contains(names, f.getUsername()));
        }

        // 3. perform assertion with another different query
        @SuppressWarnings("unchecked")
        List<Depart> followersWithNameStartsWithA = (List<Depart>) getHibernateTemplate()
                .find("from Depart where username like ?", "A%");
        assertTrue(CollectionUtils.isNotEmpty(followersWithNameStartsWithA));
        assertEquals(3, followersWithNameStartsWithA.size());
        for (Depart f : followersWithNameStartsWithA) {
            assertTrue(ArrayUtils.contains(names, f.getUsername()));
        }

        @SuppressWarnings("unchecked")
        List<Depart> followersWithNameStartsWithD = (List<Depart>) getHibernateTemplate()
        		.find("from Depart where username like ?", "D%");
        assertTrue(CollectionUtils.isEmpty(followersWithNameStartsWithD));
    }

    /**
     * adapted from {@link CobarHibernateTemplateWithNamespaceRouterTest}
     * with some adjustments.
     */
    public void testQueryForObjectOnCobarHibernateTemplate() {
        // 1. initialize data
        String[] names = { "Aaron", "Amily", "Aragon" };
        batchInsertMultipleDepartsAsFixture(names);

        // 2. assertion.
        for (String name : names) {
            List<Depart> list = (List<Depart>) getHibernateTemplate().find("from Depart where username = ?", new String[]{name});
            Depart d = list.isEmpty() ? null : list.get(0) ;
            if (name.startsWith("A")) {
                assertNotNull(d);
            } else {
                assertNull(d);
            }
        }
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
            if (name.startsWith("A")) {
            	List<Depart> list = (List<Depart>) getHibernateTemplate().find("from Depart where username = ?", new String[]{name});
            	Depart d = list.isEmpty() ? null : list.get(0) ;
                assertNotNull(d);
                d.setUsername(d.getUsername() + nameSuffix);
                getHibernateTemplate().update(d);

                int id = d.getId();

                d = null;
                list.clear();
                list = (List<Depart>) getHibernateTemplate().find("from Depart where username = ?", new String[]{name});
                d = list.isEmpty() ? null : list.get(0) ;
                assertNull(d);

                d = (Depart) getHibernateTemplate().get(Depart.class, id);
                assertNotNull(d);
                assertEquals(name + nameSuffix, d.getUsername());
            } else {
                String sql = "select * from depart where username=?";
                RowMapper rowMapper = new RowMapper() {
                    public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                        Depart d = new Depart();
                        d.setId(rs.getInt(1));
                        d.setUsername(rs.getString(2));
                        return d;
                    }
                };
                Depart d = (Depart) jt2m.queryForObject(sql, new Object[] { name },
                        rowMapper);
                assertNotNull(d);
                d.setUsername(d.getUsername() + nameSuffix);
                getHibernateTemplate().update(d);

                int id = d.getId();

                d = null;
                try {
                    d = (Depart) jt2m
                            .queryForObject(sql, new Object[] { name }, rowMapper);
                    fail();
                } catch (DataAccessException e) {
                    assertTrue(e instanceof EmptyResultDataAccessException);
                }

                d = (Depart) jt2m.queryForObject("select * from depart where id=?",
                        new Object[] { id }, rowMapper);
                assertNotNull(d);
                assertEquals(name + nameSuffix, d.getUsername());
            }
        }
    }

}
