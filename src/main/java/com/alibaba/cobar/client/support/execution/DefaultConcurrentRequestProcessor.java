/**
 * Copyright 1999-2011 Alibaba Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.alibaba.cobar.client.support.execution;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.sql.DataSource;
import javax.sql.rowset.CachedRowSet;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;
import org.springframework.orm.hibernate3.HibernateCallback;
import org.springframework.orm.hibernate3.SessionFactoryUtils;
import org.springframework.util.Assert;

import com.alibaba.cobar.client.support.utils.CollectionUtils;
import com.sun.rowset.CachedRowSetImpl;

public class DefaultConcurrentRequestProcessor implements IConcurrentRequestProcessor {

    private transient final Logger logger = LoggerFactory
                                                  .getLogger(DefaultConcurrentRequestProcessor.class);

    private SessionFactory sessionFactory;

    public DefaultConcurrentRequestProcessor() {
    }

    public DefaultConcurrentRequestProcessor(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }
    
    public List<Object> process(List<ConcurrentRequest> requests) {
        List<Object> resultList = new ArrayList<Object>();

        if (CollectionUtils.isEmpty(requests))
            return resultList;

        List<RequestDepository> requestsDepo = fetchConnectionsAndDepositForLaterUse(requests);
        final CountDownLatch latch = new CountDownLatch(requestsDepo.size());
        List<Future<Object>> futures = new ArrayList<Future<Object>>();
        try {
            for (RequestDepository rdepo : requestsDepo) {
                ConcurrentRequest request = rdepo.getOriginalRequest();
                final HibernateCallback action = request.getAction();
                final Connection connection = rdepo.getConnectionToUse();
                
                futures.add(request.getExecutor().submit(new Callable<Object>() {
                    public Object call() throws Exception {
                        try {
                            return executeWith(connection, action, false, true );
                        } finally {
                            latch.countDown();
                        }
                    }
                }));
            }
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new ConcurrencyFailureException(
                        "interrupted when processing data access request in concurrency", e);
            }

        } finally {
            for (RequestDepository depo : requestsDepo) {
                Connection springCon = depo.getConnectionToUse();
                DataSource dataSource = depo.getOriginalRequest().getDataSource();
                try {
                    if (springCon != null && !springCon.isClosed()) {
                        if (depo.isTransactionAware()) {
                            springCon.close();
                        } else {
                            DataSourceUtils.doReleaseConnection(springCon, dataSource);
                        }
                    }
                } catch (Throwable ex) {
                    logger.info("Could not close JDBC Connection", ex);
                }
            }
        }
        fillResultListWithFutureResults(futures, resultList);
        return resultList;
    }

    private void fillResultListWithFutureResults(List<Future<Object>> futures,List<Object> resultList) {
        for (Future<Object> future : futures) {
            try {
                resultList.add(future.get());
            } catch (InterruptedException e) {
                throw new ConcurrencyFailureException(
                        "interrupted when processing data access request in concurrency", e);
            } catch (ExecutionException e) {
                throw new ConcurrencyFailureException("something goes wrong in processing", e);
            }
        }
    }

    private List<RequestDepository> fetchConnectionsAndDepositForLaterUse(List<ConcurrentRequest> requests) {
        List<RequestDepository> depos = new ArrayList<RequestDepository>();
        for (ConcurrentRequest request : requests) {
            DataSource dataSource = request.getDataSource();

            Connection springCon = null;
            boolean transactionAware = (dataSource instanceof TransactionAwareDataSourceProxy);
            try {
                springCon = (transactionAware ? dataSource.getConnection() : DataSourceUtils.doGetConnection(dataSource));
            } catch (SQLException ex) {
                throw new CannotGetJdbcConnectionException("Could not get JDBC Connection", ex);
            }

            RequestDepository depo = new RequestDepository();
            depo.setOriginalRequest(request);
            depo.setConnectionToUse(springCon);
            depo.setTransactionAware(transactionAware);
            depos.add(depo);
        }
        return depos;
    }
    
    private List<RequestDepositoryForSQL> fetchConnectionsAndDepositForSQLLaterUse(List<ConcurrentRequestForSQL> requests) {
        List<RequestDepositoryForSQL> depos = new ArrayList<RequestDepositoryForSQL>();
        int count = 0 ;
        for (ConcurrentRequestForSQL request : requests) {
        	count ++ ;
        	DataSource dataSource = request.getDataSource();

            Connection springCon = null;
            PreparedStatement pstmt = null ;
            boolean transactionAware = (dataSource instanceof TransactionAwareDataSourceProxy);
            try {
                springCon = (transactionAware ? dataSource.getConnection() : DataSourceUtils.doGetConnection(dataSource));
            } catch (SQLException ex) {
                throw new CannotGetJdbcConnectionException("Could not get JDBC Connection", ex);
            }
            try {
            	pstmt = springCon.prepareStatement(request.getSql()) ;
            	Object obj = request.getParam() ;
            	if(obj != null){
	            	if(obj instanceof Collection){
	            		List<Map<String, String>> listMap = (List<Map<String, String>>)obj;
	            		for(Map<String, String> map : listMap){
	            			if(map != null && map.size() > 0){
	            				for (Entry<String , String> entry : map.entrySet()){
		    		                pstmt.setObject(Integer.parseInt(entry.getKey()) + 1, entry.getValue());
		    		            }
		    		            pstmt.addBatch();
		    		            //map.clear();
	            			}
	            		}
	            		//if(count == requests.size())
	            		//	listMap.clear();
	            	}else {
	            		Map<String, String> map = (Map<String, String>)obj ;
	    				if (map.size() != 0){
	    		            for (Entry<String , String> entry : map.entrySet()){
	    		                pstmt.setObject(Integer.parseInt(entry.getKey()) + 1, entry.getValue());
	    		            }
	    		            //if(count == requests.size())
	    		            //	map.clear();
	    			    }
	            	}
            	}
			} catch (SQLException e) {
				e.printStackTrace();
			}
            
            RequestDepositoryForSQL depo = new RequestDepositoryForSQL();
            depo.setOriginalRequest(request);
            depo.setConnectionToUse(springCon);
            depo.setPs(pstmt);
            depo.setTransactionAware(transactionAware);
            depos.add(depo);
        }
        return depos;
    }
    
    private Object executeWith(Connection connection, HibernateCallback action, boolean enforceNewSession,
			boolean enforceNativeSession) {
    	Assert.notNull(action, "Callback object must not be null");
    	Session session = null ;
    	try {
			logger.error("Current Datasource Connection Url is : " + connection.getMetaData().getURL());
			session = getSessionFactory().openSession(connection) ;
		} catch (SQLException ex) {
			throw new CannotGetJdbcConnectionException("Could not get JDBC Connection", ex);
		}
        try {
            try {
            	Object result =action.doInHibernate(session);
                session.flush();
                return result ;
            } catch (SQLException ex) {
                throw new SQLErrorCodeSQLExceptionTranslator().translate("Hibernate operation",
                        null, ex);
            }
        } finally {
        	SessionFactoryUtils.closeSession(session);
        }
    }

	public SessionFactory getSessionFactory() {
		return sessionFactory;
	}

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	@Override
	public List<Object> processQuerySQL(List<ConcurrentRequestForSQL> requests) {
		List<Object> resultList = new ArrayList<Object>();

        if (CollectionUtils.isEmpty(requests))
            return resultList;

        List<RequestDepositoryForSQL> requestsDepo = fetchConnectionsAndDepositForSQLLaterUse(requests);
        final CountDownLatch latch = new CountDownLatch(requestsDepo.size());
        List<Future<Object>> futures = new ArrayList<Future<Object>>();
        try {
            for (RequestDepositoryForSQL rdepo : requestsDepo) {
                ConcurrentRequestForSQL request = rdepo.getOriginalRequest();
                final String sql = request.getSql();
                final Connection connection = rdepo.getConnectionToUse();
                final java.sql.PreparedStatement pstm = rdepo.getPs() ;
                futures.add(request.getExecutor().submit(new Callable<Object>() {
                    public Object call() throws Exception {
                    	ResultSet rs = null ;
                    	CachedRowSet rowset = null;
                        try {
                        	rs = pstm.executeQuery();
        					rowset = new CachedRowSetImpl();
        					rowset.populate(rs);
                            return rowset ;
                        } finally {
                        	latch.countDown();
                        	if (rs != null)
                						rs.close();
                        	if (pstm != null)
                						pstm.close();
                        }
                    }
                }));
            }
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new ConcurrencyFailureException(
                        "interrupted when processing data access request in concurrency", e);
            }

        } finally {
            for (RequestDepositoryForSQL depo : requestsDepo) {
                Connection springCon = depo.getConnectionToUse();
                DataSource dataSource = depo.getOriginalRequest().getDataSource();
                try {
                    if (springCon != null && !springCon.isClosed()) {
                        if (depo.isTransactionAware()) {
                            springCon.close();
                        } else {
                            DataSourceUtils.doReleaseConnection(springCon, dataSource);
                        }
                    }
                } catch (Throwable ex) {
                    logger.info("Could not close JDBC Connection", ex);
                }
            }
        }
        fillResultListWithFutureResults(futures, resultList);
        return resultList;
	}

	@Override
	public Object processBatchUpdateSQL(List<ConcurrentRequestForSQL> requests) {
        if (CollectionUtils.isEmpty(requests))
            return false ;

        List<RequestDepositoryForSQL> requestsDepo = fetchConnectionsAndDepositForSQLLaterUse(requests);
        final CountDownLatch latch = new CountDownLatch(requestsDepo.size());
        //预留返回值
        List<Future<Object>> futures = new ArrayList<Future<Object>>();
        try {
            for (RequestDepositoryForSQL rdepo : requestsDepo) {
                ConcurrentRequestForSQL request = rdepo.getOriginalRequest();
                final Connection connection = rdepo.getConnectionToUse();
                final java.sql.PreparedStatement pstm = rdepo.getPs() ;
                futures.add(request.getExecutor().submit(new Callable<Object>() {
                    public Object call() throws Exception {
                    	connection.setAutoCommit(false);
                        try {
                        	Object object = pstm.executeBatch() ;
                        	connection.commit();
                        	return object ;
                        } catch(Exception e) {
                        	connection.rollback();
                        	e.printStackTrace();
                        	throw e ;
                        } finally {
                        	latch.countDown();
                        	if (pstm != null)
                				pstm.close();
                        	if (connection != null)
                        		connection.close();
                        }
                    }
                }));
            }
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new ConcurrencyFailureException(
                        "interrupted when processing data access request in concurrency", e);
            }

        } catch(Exception e) {
        	e.printStackTrace();
        } finally {
            for (RequestDepositoryForSQL depo : requestsDepo) {
                Connection springCon = depo.getConnectionToUse();
                DataSource dataSource = depo.getOriginalRequest().getDataSource();
                try {
                    if (springCon != null) {
                        if (depo.isTransactionAware()) {
                            springCon.close();
                        } else {
                            DataSourceUtils.doReleaseConnection(springCon, dataSource);
                        }
                    }
                } catch (Throwable ex) {
                    logger.info("Could not close JDBC Connection", ex);
                }
            }
        }
        return true ;
	}

}
