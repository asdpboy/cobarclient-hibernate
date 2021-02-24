package com.alibaba.cobar.client;

import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.fail;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.hibernate3.HibernateTemplate;
import org.testng.annotations.AfterClass;

public abstract class AbstractTestNGCobarClientTest {
    
    private transient final Logger logger = LoggerFactory
            .getLogger(AbstractTestNGCobarClientTest.class);
    
    public static final String   CREATE_TABLE_USER    = "CREATE TABLE IF NOT EXISTS user(id BIGINT PRIMARY KEY, username VARCHAR(140), password VARCHAR(140))";
    public static final String   CREATE_TABLE_ROLE      = "CREATE TABLE IF NOT EXISTS role(id BIGINT PRIMARY KEY, username VARCHAR(140), password VARCHAR(140))";

    public static final String   TRUNCATE_TABLE_USER   = "TRUNCATE TABLE user";
    public static final String   TRUNCATE_TABLE_ROLE    = "TRUNCATE TABLE role";

    private ApplicationContext   applicationContext;
    private HibernateTemplate hibernateTemplate;
    protected JdbcTemplate       jt1m;
    protected JdbcTemplate       jt1s;
    protected JdbcTemplate       jt2m;
    protected JdbcTemplate       jt2s;

    public AbstractTestNGCobarClientTest(String[] locations) {
        applicationContext = new ClassPathXmlApplicationContext(locations);
        ((AbstractApplicationContext) applicationContext).registerShutdownHook();

        setHibernateTemplate((HibernateTemplate) applicationContext.getBean("hibernateTemplate"));

        jt1m = new JdbcTemplate((DataSource) applicationContext.getBean("partition1_main"));
        jt1s = new JdbcTemplate((DataSource) applicationContext.getBean("partition1_standby"));
        jt2m = new JdbcTemplate((DataSource) applicationContext.getBean("partition2_main"));
        jt2s = new JdbcTemplate((DataSource) applicationContext.getBean("partition2_standby"));

        jt1m.execute(CREATE_TABLE_USER);
        jt1m.execute(CREATE_TABLE_ROLE);

        jt1s.execute(CREATE_TABLE_USER);
        jt1s.execute(CREATE_TABLE_ROLE);

        jt2m.execute(CREATE_TABLE_USER);
        jt2m.execute(CREATE_TABLE_ROLE);

        jt2s.execute(CREATE_TABLE_USER);
        jt2s.execute(CREATE_TABLE_ROLE);
    }

    //@BeforeMethod(alwaysRun = true)
    public void setUpBeforeEachTestMethodRun() {
        jt1m.execute(TRUNCATE_TABLE_USER);
        jt1m.execute(TRUNCATE_TABLE_ROLE);

        jt1s.execute(TRUNCATE_TABLE_USER);
        jt1s.execute(TRUNCATE_TABLE_ROLE);

        jt2m.execute(TRUNCATE_TABLE_USER);
        jt2m.execute(TRUNCATE_TABLE_ROLE);

        jt2s.execute(TRUNCATE_TABLE_USER);
        jt2s.execute(TRUNCATE_TABLE_ROLE);
    }

    @AfterClass
    public void cleanup() {
        if (applicationContext != null) {
            logger.info("shut down Application Context to clean up.");            
            ((AbstractApplicationContext) applicationContext).destroy();
        }
    }
    
    protected void batchInsertMultipleDepartsAsFixtureWithJdbcTemplate(String[] names,
                                                                         JdbcTemplate jt) {
        for (String name : names) {
            String sql = "insert into followers(name) values('" + name + "')";
            jt.update(sql);
        }
    }

    protected void verifyEntityNonExistenceOnSpecificDataSource(String sql, JdbcTemplate jt) {
        try {
            jt.queryForObject(sql, String.class);
            fail();
        } catch (DataAccessException e) {
            // pass
        }
    }

    protected void verifyEntityExistenceOnSpecificDataSource(String sql, JdbcTemplate jt) {
        try {
            assertNotNull(jt.queryForObject(sql, String.class));
        } catch (DataAccessException e) {
            fail();
        }
    }

    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    public void setHibernateTemplate(HibernateTemplate hibernateTemplate) {
        this.hibernateTemplate = hibernateTemplate;
    }

    public HibernateTemplate getHibernateTemplate() {
        return hibernateTemplate;
    }
}
