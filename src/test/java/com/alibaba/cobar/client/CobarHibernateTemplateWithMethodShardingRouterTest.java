package com.alibaba.cobar.client;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.alibaba.cobar.client.entities.Depart;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.testng.annotations.Test;

import com.alibaba.cobar.client.support.utils.CollectionUtils;
@Test(sequential=true)
public class CobarHibernateTemplateWithMethodShardingRouterTest extends
        AbstractTestNGCobarClientTest {

    private transient final Logger logger = LoggerFactory
                                                  .getLogger(CobarHibernateTemplateWithMethodShardingRouterTest.class);

    private String[]               names  = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };

    public CobarHibernateTemplateWithMethodShardingRouterTest() {
        super(new String[] { "META-INF/spring/cobar-client-appctx.xml",
                "META-INF/spring/datasources-appctx.xml",
                "META-INF/spring/sqlaction-sharding-router-appctx.xml" });
    }
    
    public void testInsertOnCobarSqlMapClientTemplateWithDepartA() {
        String name = "Aranda";
        Depart depart = new Depart(1, name);
        getHibernateTemplate().save(depart);

        String confirmSQL = "select username from depart where username='" + name + "'";
        verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
    }

    public void testInsertOnCobarSqlMapClientTemplateWithDepartD() {
        String name = "Darl";
        Depart depart = new Depart(2, name);
        getHibernateTemplate().save(depart);

        String confirmSQL = "select username from depart where username='" + name + "'";
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
    }

    public void testInsertOnCobarSqlMapClientTemplateWithDepartNonAorD() {
        String name = "Sara";
        Depart depart = new Depart(3, name);
        getHibernateTemplate().save(depart);

        String confirmSQL = "select username from depart where username='" + name + "'";
        verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
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
    
    public void testInsertWithBatchOnCobarSqlMapClientTemplate() {
        batchInsertMultipleDepartsAsFixture(names);
        for (String name : names) {
            String confirmSQL = "select username from depart where username='" + name + "'";
            if (name.startsWith("A")) {
                verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
            }
            if (name.startsWith("D")) {
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
                verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
            }
        }
        @SuppressWarnings("unchecked")
        List<Depart> departs = (List<Depart>) getHibernateTemplate().find("from Depart");
        // since no routing rule is defined for 'findAll', it can only find out records on default data source
        assertTrue(CollectionUtils.isNotEmpty(departs));
        assertEquals(3, departs.size());
        for (Depart depart : departs) {
            assertTrue(ArrayUtils.contains(names, depart.getUsername()));
        }

        assertEquals(2, jt2m.queryForInt("select count(1) from depart"));
    }

    public void testInsertWithBatchOnCobarSqlMapClientTemplateWithDataHavingNoRuleQualifiedFor() {
        List<String> nameList = new ArrayList<String>();
        nameList.addAll(Arrays.asList(names));
        nameList.add("Sara");
        nameList.add("Samansa");
        batchInsertMultipleDepartsAsFixture(nameList.toArray(new String[nameList.size()]));

        for (String name : nameList) {
            String confirmSQL = "select username from depart where username='" + name + "'";
            if (name.startsWith("D")) {
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
                verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
            } else {
                verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
            }
        }

        @SuppressWarnings("unchecked")
        List<Depart> departs = (List<Depart>) getHibernateTemplate().find("from Depart");
        // since 'findAll' action will be performed only against default data source(partition1), 
        // it will only find records whose 'name' column has value that doesn't start with 'D'
        assertTrue(CollectionUtils.isNotEmpty(departs));
        assertEquals(5, departs.size());
        for (Depart depart : departs) {
            assertTrue(nameList.contains(depart.getUsername()));
        }

        assertEquals(2, jt2m.queryForInt("select count(1) from depart"));
    }

    public void testDeleteOnCobarSqlMapClientTemplate() {
        batchInsertMultipleDepartsAsFixture(names);
        for (String name : names) {
            String confirmSQL = "select username from depart where username='" + name + "'";
            if (name.startsWith("A")) {
                verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
            }
            if (name.startsWith("D")) {
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
                verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
            }
        }

        // no rules are defined for deletion, so it will be only performed on default data source(partition1).
        
        for (String name : names) {
        	Depart depart = new Depart() ;
        	depart.setUsername(name);
            getHibernateTemplate().delete(depart);
        }
    }

    @SuppressWarnings("unchecked")
    public void testQueryForListOnCobarSqlMapClientTemplateWithPartialDataOnDefaultDataSource() {
        batchInsertMultipleDepartsAsFixture(names);

        List<Depart> departs = (List<Depart>) getHibernateTemplate().find("from Depart");
        assertTrue(CollectionUtils.isNotEmpty(departs));
        // only departs with name that starts with "A" can be found
        assertEquals(3, departs.size());
        for (Depart depart : departs) {
            assertTrue(depart.getUsername().startsWith("A"));
        }

        departs = null;
        departs = (List<Depart>) getHibernateTemplate().find("from Depart where username like ?", "A%");
        assertTrue(CollectionUtils.isNotEmpty(departs));
        // only departs with name that starts with "A" can be found
        assertEquals(3, departs.size());
        for (Depart depart : departs) {
            assertTrue(depart.getUsername().startsWith("A"));
        }

        departs = null;
        departs = (List<Depart>) getHibernateTemplate().find("from Depart where username like ?", "D%");
        assertTrue(CollectionUtils.isEmpty(departs));

        departs = null;
        departs = (List<Depart>) getHibernateTemplate().find("from Depart where username like ?", "S%");
        assertTrue(CollectionUtils.isEmpty(departs));
    }

    public void testQueryForObjectOnCobarSqlMapClientTemplate() {
        batchInsertMultipleDepartsAsFixture(names);

        for (String name : names) {
        	List<Depart> departs = (List<Depart>) getHibernateTemplate().find("from Depart where username = ?", new String[]{name});
            Depart depart = departs.isEmpty() ? null : departs.get(0) ;
            if (name.startsWith("A")) {
                assertNotNull(depart);
            }
            if (name.startsWith("D")) {
                assertNull(depart);
            }
        }

        String name = "Jesus";
        int count = jt2m.update("insert into departs(name) values(?)", new Object[] { name });
        if (count == 1) {
        	List<Depart> departs = (List<Depart>) getHibernateTemplate().find("from Depart where username = ?", new String[]{name});
            Depart depart = departs.isEmpty() ? null : departs.get(0) ;
            assertNull(depart);
        }

        Depart depart = new Depart(20 ,name);
        Object pk = getHibernateTemplate().save(depart);
        if (pk != null) {
        	List<Depart> departs = (List<Depart>) getHibernateTemplate().find("from Depart where username = ?", new String[]{name});
            Depart d = departs.isEmpty() ? null : departs.get(0) ;
            assertNotNull(d);
            assertEquals(name, d.getUsername());
        } else {
            logger.warn("failed to create fixture Depart object.");
        }
    }
    /**
     * WARNING: don't do stupid things such like below, we do this because we
     * can guarantee the shard id will NOT change. if you want to use cobar
     * client corretly, make sure you are partitioning you databases with shard
     * id that will not be changed once it's created!!!
     */
    public void testUpdateOnCobarSqlMapClientTemplate(){
        batchInsertMultipleDepartsAsFixture(names);
        
        List<Depart> departsToUpdate = new ArrayList<Depart>();
        for(String name:names)
        {
        	List<Depart> departs = (List<Depart>) getHibernateTemplate().find("from Depart where username = ?", new String[]{name});
            Depart depart = departs.isEmpty() ? null : departs.get(0) ;
            if(depart != null){
                departsToUpdate.add(depart);
            }
            else{
                depart = (Depart)jt2m.queryForObject("select * from depart where username=?", new Object[]{name}, new RowMapper(){
                public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                    Depart d = new Depart();
                    d.setId(rs.getInt(1));
                    d.setUsername(rs.getString(2));
                    return d;
                }});
                departsToUpdate.add(depart);
            }
        }
        assertEquals(5, departsToUpdate.size());
        
        for(Depart depart:departsToUpdate)
        {
            depart.setUsername(depart.getUsername().toUpperCase());
            getHibernateTemplate().update(depart);
            if(depart.getUsername().startsWith("A")){
                Depart f = (Depart)getHibernateTemplate().load(Depart.class ,depart.getId());
                assertEquals(depart.getUsername(), f.getUsername());
            }
            else {
                //
            }
        }
        
        
    }
}
