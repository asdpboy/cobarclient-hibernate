package com.alibaba.cobar.client;

import java.util.List;

import com.alibaba.cobar.client.entities.Offer;
import org.testng.annotations.Test;

@Test(sequential=true)
public class CobarHibernateTemplateWithCompositeIDRouterTest extends AbstractTestNGCobarClientTest {

    public CobarHibernateTemplateWithCompositeIDRouterTest() {
        super(new String[] { "META-INF/spring/cobar-client-appctx.xml",
                "META-INF/spring/datasources-appctx.xml",
                "META-INF/spring/classname-router-appctx.xml" });
    }

    public void testInsertOnCobarHibernateTemplate() {
    	/*
        Offer offer = new Offer();
        offer.setId(1l);
        offer.setMemberId("1memberId");
        offer.setSubject("1subject");
        getHibernateTemplate().save(offer ) ;
        
        */
    	
    	List<Offer> offers = getHibernateTemplate().find("from Offer where memberId = ?" , new String[]{"1memberId"}) ;
        if(!offers.isEmpty()){
        	System.out.println(offers.get(0));
        }
    }

    public void testInsertInBatchOnCobarHibernateTemplate() {
 
        
    }
    

    protected void batchInsertMultipleDepartsAsFixture(String[] names) {

    }

    public void testDeleteOnCobarSqlMapClientTemplate() {
    	
    }

    public void testDeleteWithExpectedResultSizeOnCobarHibernateTemplate() {
    	
    }

    public void testQueryForListOnCobarHibernateTemplate() {
       
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
       
    }

}
