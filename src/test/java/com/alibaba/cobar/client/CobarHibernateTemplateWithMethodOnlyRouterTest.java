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
import org.springframework.jdbc.core.RowMapper;
import org.testng.annotations.Test;

import com.alibaba.cobar.client.support.utils.CollectionUtils;

@Test(sequential=true)
public class CobarHibernateTemplateWithMethodOnlyRouterTest extends
        AbstractTestNGCobarClientTest {

    public CobarHibernateTemplateWithMethodOnlyRouterTest() {
        super(new String[] { "META-INF/spring/cobar-client-appctx.xml",
                "META-INF/spring/datasources-appctx.xml",
                "META-INF/spring/methodname-router-appctx.xml" });
    }

    public void testInsertOnCobarHibernateWithSqlActionOnlyRules() {
        String name = "Darren";
        Depart d = new Depart(1 , name);
        getHibernateTemplate().save(d);
        // since no rule for this insert, it will be inserted into default data source, that's, partition1
        String confirmSQL = "select name from depart where username='" + name + "'";
        verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
        // this sql action is routed to partition2, so can't find any matched record.
        List<Depart> list = (List<Depart>)getHibernateTemplate().find("from Depart where username = ? ", name);
        Depart departToFind = list.isEmpty() ? null : list.get(0) ;
        assertNull(departToFind);
        // sql action below will be against all of the partitions , so we will get back what we want here
        @SuppressWarnings("unchecked")
        List<Depart> departs = (List<Depart>) getHibernateTemplate().find("from Depart");
        assertTrue(CollectionUtils.isNotEmpty(departs));
        assertEquals(1, departs.size());
        assertEquals(name, departs.get(0).getUsername());
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
    
    public void testInsertWithBatchCommitOnCobarHibernateTemplateWithMethodNameOnlyRules() {
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };
        batchInsertMultipleDepartsAsFixture(names);
        // since no routing rule for insertion, all of the records will be inserted into default data source, that's, partition1
        for (String name : names) {
            String confirmSQL = "select username from depart where username='" + name + "'";
            verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
            verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
            verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
            verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
        }
        
        // although records only reside on partition1, but we can get all of them with sql action below
        @SuppressWarnings("unchecked")
        List<Depart> departs = (List<Depart>) getHibernateTemplate().find("from Depart");
        assertTrue(CollectionUtils.isNotEmpty(departs));
        assertEquals(names.length, departs.size());
        for (Depart f : departs) {
            assertTrue(ArrayUtils.contains(names, f.getUsername()));
        }
    }

    public void testDeleteOnCobarHibernateTemplate() {
        String name = "Darren";

        // insert 1 record and delete will affect this record which resides on partition1
        Depart depart = new Depart(5, name);
        getHibernateTemplate().save(depart);
        try {
        	getHibernateTemplate().delete(depart);
		} catch (Exception e) {
			fail("delete error") ;
		}
    }

    /**
     * insert data onto default data source , and query will against all of the
     * partitions, so all of the records will be returned as expected.
     */
    public void testQueryForListOnCobarHibernateTemplateNormally() {
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };
        batchInsertMultipleDepartsAsFixture(names);

        @SuppressWarnings("unchecked")
        List<Depart> departs = (List<Depart>) getHibernateTemplate().find("from Depart");
        assertTrue(CollectionUtils.isNotEmpty(departs));
        assertEquals(names.length, departs.size());
        for (Depart d : departs) {
            assertTrue(ArrayUtils.contains(names, d.getUsername()));
        }
    }

    /**
     * although records are inserted onto patition2, but since the query is
     * against all of the data sources, so all of the records will be returned
     * as expected.
     */
    public void testQueryForListOnCobarHibernateTemplateWithoutDefaultPartitionData() {
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };
        batchInsertMultipleDepartsAsFixtureWithJdbcTemplate(names, jt2m);

        @SuppressWarnings("unchecked")
        List<Depart> departs = (List<Depart>) getHibernateTemplate().find("from Depart") ;
        assertTrue(CollectionUtils.isNotEmpty(departs));
        assertEquals(names.length, departs.size());
        for (Depart f : departs) {
            assertTrue(ArrayUtils.contains(names, f.getUsername()));
        }
    }

    /**
     * insert records onto partition1, but the
     * 'com.alibaba.cobar.client.entities.Depart.finaByName' will be performed
     * against partition2 as per the routing rule, so no record will be
     * returned.
     */
    public void testQueryForObjectOnCobarHibernateTemplateWithDefaultPartition() {
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };
        batchInsertMultipleDepartsAsFixture(names);

        for (String name : names) {
            List<Depart> list = (List<Depart>)getHibernateTemplate().find("from Depart where username = ?", new String[]{name});
            Depart d = list.isEmpty() ? null : list.get(0) ;
            assertNull(d);
        }
    }

    /**
     * we insert records onto partition2, and the
     * 'com.alibaba.cobar.client.entities.Depart.finaByName' action will be
     * performed against partition2 too, so each record will be returned as
     * expected.
     */
    public void testQueryForObjectOnCobarHibernateTemplateWithFillingDataOntoPartition2() {
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };
        batchInsertMultipleDepartsAsFixtureWithJdbcTemplate(names, jt2m);

        for (String name : names) {
            List<Depart> list = (List<Depart>)getHibernateTemplate().find("from Depart where username = ?", new String[]{name});
            Depart d = list.isEmpty() ? null : list.get(0) ;
            assertNotNull(d);
            assertTrue(ArrayUtils.contains(names, d.getUsername()));
        }
    }

    /**
     * WARNING: don't do stupid things such like below, we do this because we
     * can guarantee the shard id will NOT change. if you want to use cobar
     * client corretly, make sure you are partitioning you databases with shard
     * id that will not be changed once it's created!!!
     * <br>
     * with data fixtures setting up on default data source, and update with
     * CobarHibernateTemplate.
     */
    public void testUpdateOnCobarHibernateTemplateNormally() {
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };
        batchInsertMultipleDepartsAsFixture(names);
        
        for(String name:names) {
            Depart f = (Depart)jt1m.queryForObject("select * from depart where username=?",new Object[]{name}, new RowMapper(){
                public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                    Depart d =  new Depart();
                    d.setId(rs.getInt(1));
                    d.setUsername(rs.getString(2));
                    return d;
                }});
            assertNotNull(f);
            try {
            	getHibernateTemplate().update(f);
			} catch (Exception e) {
				fail("update error");
			}
        }
    }
    /**
     * WARNING: don't do stupid things such like below, we do this because we
     * can guarantee the shard id will NOT change. if you want to use cobar
     * client corretly, make sure you are partitioning you databases with shard
     * id that will not be changed once it's created!!!
     * <br>
     * with data fixtures setting up on another data source, and update with
     * CobarHibernateTemplate.
     */
    public void testUpdateOnCobarHibernateTemplateAbnormally() {
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };
        batchInsertMultipleDepartsAsFixtureWithJdbcTemplate(names, jt2m);
        
        for(String name:names) {
        	List<Depart> list = (List<Depart>)getHibernateTemplate().find("from Depart where username = ?", new String[]{name});
            Depart d = list.isEmpty() ? null : list.get(0) ;
            assertNotNull(d); // this sql action is performed against partition2 as per routing rule
            
            // sql action below will be performed against default data source(partition1), so will not affect any records on partition2
            try {
            	getHibernateTemplate().update(d);
			} catch (Exception e) {
				fail("update error");
			}
            
        }
    }
}
