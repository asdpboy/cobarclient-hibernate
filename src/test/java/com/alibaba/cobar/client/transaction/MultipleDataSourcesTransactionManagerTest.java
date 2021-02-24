package com.alibaba.cobar.client.transaction;


import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.testng.annotations.Test;

import com.alibaba.cobar.client.AbstractTestNGCobarClientTest;
import com.alibaba.cobar.client.entities.User;

/**
 * H2 In-Memory Database doesn't support transaction, so in this test case, we
 * need to turn to non-in-memory database to test the transaction.<br>
 * 
 * @author
 */
@Test(sequential=true)
public class MultipleDataSourcesTransactionManagerTest extends AbstractTestNGCobarClientTest {

    private String[]               memberIds             = new String[] { "1","2","3","4","5","6","7" };

    public MultipleDataSourcesTransactionManagerTest() {
        super(new String[] {
                "META-INF/spring/cobar-client-appctx.xml",
                "META-INF/spring/datasources-appctx.xml",
                "META-INF/spring/classname-methodname-composed-router-appctx.xml",
                "META-INF/spring/cobar-client-user-services-appctx.xml" });
    }
    
    /*不带返回值事务控制方法*/
    public void testUserCreationOnMultipleShardsWithTransactionRollback() {
    	try {
    		new TransactionTemplate(((PlatformTransactionManager) getApplicationContext().getBean("transactionManager"))).execute(new TransactionCallbackWithoutResult() {
                @Override
                protected void doInTransactionWithoutResult(TransactionStatus status) {
                    try {
                        User user = new User();
                        user.setId(3);
                        user.setUsername("o1");
                        user.setPassword("po1");
                        getHibernateTemplate().save(user);

                        User user1 = new User();
                        user1.setId(2);
                        user1.setPassword("po2");
                        user1.setUsername("o2");
                        getHibernateTemplate().save(user1);
                        
                    } catch(Exception e){
                    	//如果出现异常，回滚事务即可
                    	status.setRollbackOnly();
                    	throw new RuntimeException(e) ;
                    }
                }
            });
		} catch (Exception e) {
			//e.printStackTrace();
			System.out.println("ERROR : " + e.getMessage());
		}
        
    }
    
    /*带返回值事务控制方法*/
    public Object testUserCreationOnMultipleShardsWithResultTransactionRollback() {
       return new TransactionTemplate(((PlatformTransactionManager) getApplicationContext().getBean("transactionManager"))).execute(new TransactionCallback() {
            @Override
			public Object doInTransaction(TransactionStatus status) {
                try {
                    User user = new User();
                    user.setId(3);
                    user.setUsername("o1");
                    user.setPassword("po1");
                    getHibernateTemplate().save(user);

                    User user1 = new User();
                    user1.setId(2);
                    user1.setPassword("po2");
                    user1.setUsername("o2");
                    getHibernateTemplate().save(user1);
                } catch(Exception e){
                	e.printStackTrace();
                	//如果出现异常，回滚事务即可
                	status.setRollbackOnly();
                }
                return "" ;
            }
        });
    }
}
