package com.alibaba.cobar.client;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;
import javax.sql.RowSet;

import com.alibaba.cobar.client.datasources.CobarDataSourceDescriptor;
import com.alibaba.cobar.client.datasources.ICobarDataSourceService;
import com.alibaba.cobar.client.router.ICobarRouter;
import com.alibaba.cobar.client.router.support.HibernateRoutingFact;
import com.alibaba.cobar.client.support.HQLBean;
import com.alibaba.cobar.client.support.execution.ConcurrentRequest;
import com.alibaba.cobar.client.support.execution.ConcurrentRequestForSQL;
import com.alibaba.cobar.client.support.execution.DefaultConcurrentRequestProcessor;
import com.alibaba.cobar.client.support.execution.IConcurrentRequestProcessor;
import com.alibaba.cobar.client.support.vo.CobarMRBase;
import com.alibaba.cobar.client.transaction.MultipleDataSourcesTransactionManager;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.hibernate.FlushMode;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.SessionFactoryImplementor;
import org.hibernate.engine.SessionImplementor;
import org.hibernate.hql.antlr.SqlTokenTypes;
import org.hibernate.hql.ast.QueryTranslatorImpl;
import org.hibernate.hql.ast.SqlGenerator;
import org.hibernate.hql.ast.tree.DeleteStatement;
import org.hibernate.hql.ast.tree.FromElement;
import org.hibernate.hql.ast.tree.QueryNode;
import org.hibernate.hql.ast.tree.UpdateStatement;
import org.hibernate.hql.ast.util.ASTPrinter;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Property;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.orm.hibernate3.HibernateCallback;
import org.springframework.orm.hibernate3.HibernateTemplate;
import org.springframework.orm.hibernate3.LocalSessionFactoryBean;
import org.springframework.orm.hibernate3.SessionFactoryUtils;
import org.springframework.util.Assert;

import antlr.RecognitionException;
import antlr.collections.AST;

import com.alibaba.cobar.client.audit.ISqlAuditor;
import com.alibaba.cobar.client.merger.IMerger;
import com.alibaba.cobar.client.support.utils.CollectionUtils;
import com.alibaba.cobar.client.support.utils.MapUtils;
import com.alibaba.cobar.client.support.utils.Predicate;
import com.alibaba.cobar.client.support.utils.SQLParserUtils;
import com.alibaba.cobar.client.support.utils.SimilarityUtils;

/**
 * {@link CobarHibernateTemplate} is an extension to spring's default
 * {@link HibernateTemplate}, it works as the main component of <i>Cobar
 * Client</i> product.<br>
 * 重写了{@link HibernateTemplate}对外开放的HQL操作方法，用于支持分库操作。
 * @author yanhongqi
 * @since 1.0
 * @see MultipleDataSourcesTransactionManager for transaction management
 *      alternative.
 */
public class CobarHibernateTemplate extends HibernateTemplate implements
		DisposableBean, ApplicationContextAware {

	private transient Logger logger = LoggerFactory
			.getLogger(CobarHibernateTemplate.class);

	private static final String DEFAULT_DATASOURCE_IDENTITY = "_CobarHibernateTemplate_default_data_source_name";

	private String defaultDataSourceName = DEFAULT_DATASOURCE_IDENTITY;

	private static final String REGEX_WHERE_CONDITION = "(\\w+\\.{0,1}\\w+\\s{0,5}+=((\\s{0,5}:\\w+\\.{0,1}\\w+)|\\?))";

	private static final String DELETEALL_METHOD_NAME = "deleteAll";

	private static final String SAVEORUPDATEALL_METHOD_NAME = "saveOrUpdateAll";

	private List<ExecutorService> internalExecutorServiceRegistry = new ArrayList<ExecutorService>();
	/**
	 * if we want to access multiple database partitions, we need a collection
	 * of data source dependencies.<br>
	 * {@link ICobarDataSourceService} is a consistent way to get a collection
	 * of data source dependencies for @{link CobarHibernateTemplate} and
	 * {@link MultipleDataSourcesTransactionManager}.<br>
	 * If a router is injected, a dataSourceLocator dependency should be
	 * injected too. <br>
	 */
	private ICobarDataSourceService cobarDataSourceService;

	/**
	 * To enable database partitions access, an {@link ICobarRouter} is a must
	 * dependency.<br>
	 * if no router is found, the CobarHibernateTemplate will act with behaviors
	 * like its parent, the HibernateTemplate.
	 */
	private ICobarRouter<HibernateRoutingFact> router;

	/**
	 * if you want to do SQL auditing, inject an {@link ISqlAuditor} for use.<br>
	 * a sibling ExecutorService would be prefered too, which will be used to
	 * execute {@link ISqlAuditor} asynchronously.
	 */
	private ISqlAuditor sqlAuditor;

	private ExecutorService sqlAuditorExecutor;

	/**
	 * setup ExecutorService for data access requests on each data sources.<br>
	 * map key(String) is the identity of DataSource; map value(ExecutorService)
	 * is the ExecutorService that will be used to execute query requests on the
	 * key's data source.
	 */
	private Map<String, ExecutorService> dataSourceSpecificExecutors = new HashMap<String, ExecutorService>();

	/**
	 * init context put the table name and class name in map
	 */
	private Map<String, String> tableAndClassMappers = new HashMap<String, String>();

	private final String CLASSNAME_SPLIT_STR = ",";
	
	/**
	 * init Hibernate Configuration Object
	 */
	private Configuration configuration = null;

	private ApplicationContext context = null;

	private IConcurrentRequestProcessor concurrentRequestProcessor;

	/**
	 * timeout threshold to indicate how long the concurrent data access request
	 * should time out.<br>
	 * time unit in milliseconds.<br>
	 */
	private int defaultQueryTimeout = 100;
	/**
	 * indicator to indicate whether to log/profile long-time-running SQL
	 */
	private boolean profileLongTimeRunningSql = false;
	private long longTimeRunningSqlIntervalThreshold;

	/**
	 * In fact, application can do data-merging in their application code after
	 * getting the query result, but they can let {@link CobarHibernateTemplate}
	 * do this for them too, as long as they provide a relationship mapping
	 * between the sql action and the merging logic provider.
	 */
	private Map<String, IMerger<Object, Object>> mergers = new HashMap<String, IMerger<Object, Object>>();

	/**
	 * @description 如果是通过此方法进行count操作，且对多个数据源进行了操作，则会返回两条rowset，每一条中都存在一个count值，
	 *              此方法不考虑对count进行汇总，需要业务端进行判断处理
	 * @param sql
	 * @param param
	 * @return
	 */
	public Object excuteWithQueryNativeSQL(final String sql, final Map param) {
		long startTimestamp = System.currentTimeMillis();
		try {
			if (isPartitioningBehaviorEnabled()) {
				HQLBean hqlBean = getHQLBeanBySql(sql);
				Object entity = hqlBean2Entity(hqlBean,
						getParameters(hqlBean.getSql(), null), param, false , false);
				SortedMap<String, DataSource> dsMap = lookupDataSourcesByRouter(
						hqlBean.getClassName() + ".qnative", entity);
				if (logger.isDebugEnabled())
					logger.debug("dsMap size is : " + dsMap.size());
				if (!MapUtils.isEmpty(dsMap)) {
					List<Object> originalResultList = executeSQLInConcurrency(
							sql, param, dsMap);
					int i = 0;
					RowSet totalRowSet = null;
					ResultSetMetaData rsmd = null;
					int columnCount = 1;
					try {
						for (Object object : originalResultList) {
							RowSet rowset = (RowSet) object;
							if (i == 0) {
								rsmd = rowset.getMetaData();
								columnCount = rsmd.getColumnCount();
								totalRowSet = rowset;
							} else {
								while (rowset.next()) {
									totalRowSet.last();
									totalRowSet.moveToInsertRow();
									for (int j = 0; j < columnCount; j++) {
										Object o = rowset.getObject(rsmd
												.getColumnName(j + 1));
										totalRowSet.updateObject(
												rsmd.getColumnName(j + 1), o);
									}
									totalRowSet.insertRow();
									totalRowSet.moveToCurrentRow();
								}
								totalRowSet.beforeFirst();
							}
							i++;
						}
					} catch (SQLException e) {
						e.printStackTrace();
					}
					return totalRowSet;
				}
			}
		} finally {
			if (isProfileLongTimeRunningSql()) {
				long interval = System.currentTimeMillis() - startTimestamp;
				if (interval > getLongTimeRunningSqlIntervalThreshold()) {
					logger.warn(
							"SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.",
							new Object[] { sql, param.values(), interval });
				}
			}
		}
		return null;
	}

	public Object excuteWithBatchUpdateNativeSQL(final String sql,
			final List<Map> listParam) {
		Set<String> keys = new HashSet<String>();
		keys.add(getDefaultDataSourceName());
		keys.addAll(getCobarDataSourceService().getDataSources().keySet());
		final CobarMRBase mrbase = new CobarMRBase(keys);
		ExecutorService executor = createCustomExecutorService(Runtime
				.getRuntime().availableProcessors(),
				"excuteWithBatchUpdateNativeSQL");
		try {
			final StringBuffer exceptionStaktrace = new StringBuffer();
			Collection<Map> paramCollection = listParam;
			final CountDownLatch latch = new CountDownLatch(
					paramCollection.size());
			Iterator<Map> iter = paramCollection.iterator();
			while (iter.hasNext()) {
				final Map map = iter.next();
				Runnable task = new Runnable() {
					public void run() {
						try {
							HQLBean hqlBean = getHQLBeanBySql(sql);
							Object entity = hqlBean2Entity(hqlBean,
									getParameters(hqlBean.getSql(), null), map,
									false, false);
							SortedMap<String, DataSource> dsMap = lookupDataSourcesByRouter(
									hqlBean.getClassName() + ".bunative",
									entity);
							if (MapUtils.isEmpty(dsMap)) {
								logger.info(
										"Method excuteWithBatchUpdateNativeSQL can't find routing rule for {} with parameter {}, so use default data source for it.",
										entity.getClass().getName()
												+ ".bunative", entity);
								mrbase.emit(getDefaultDataSourceName(), map);
							} else {
								if (dsMap.size() > 1) {
									throw new IllegalArgumentException(
											"Method excuteWithBatchUpdateNativeSQL unexpected routing result, found more than 1 target data source for current entity:"
													+ entity);
								}
								mrbase.emit(dsMap.firstKey(), map);
							}
						} catch (Throwable t) {
							exceptionStaktrace.append(ExceptionUtils
									.getFullStackTrace(t));
						} finally {
							latch.countDown();
						}
					}
				};
				executor.execute(task);
			}
			try {
				latch.await();
			} catch (InterruptedException e) {
				throw new ConcurrencyFailureException(
						"unexpected interruption when re-arranging parameter collection into sub-collections ",
						e);
			}

			if (exceptionStaktrace.length() > 0) {
				throw new ConcurrencyFailureException(
						"unpected exception when re-arranging parameter collection, check previous log for details.\n"
								+ exceptionStaktrace);
			}
		} finally {
			executor.shutdown();
		}
		List<ConcurrentRequestForSQL> requests = new ArrayList<ConcurrentRequestForSQL>();
		for (final Map.Entry<String, List<Object>> et : mrbase.getResources()
				.entrySet()) {
			final List<Object> paramList = et.getValue();
			if (CollectionUtils.isEmpty(paramList)) {
				continue;
			}
			String identity = et.getKey();

			final DataSource dataSourceToUse = findDataSourceToUse(identity);

			ConcurrentRequestForSQL request = new ConcurrentRequestForSQL();
			request.setDataSource(dataSourceToUse);
			request.setSql(sql);
			request.setExecutor(getDataSourceSpecificExecutors().get(identity));
			request.setParam(paramList);
			requests.add(request);
		}

		return getConcurrentRequestProcessor().processBatchUpdateSQL(requests);
	}

	/**
	 * @description
	 * @param action
	 * @param hql
	 * @return
	 */
	public Object executeWithSearchHQL(final HibernateCallback action,
			final String hql, final boolean isHQL, final HashMap args) {
		long startTimestamp = System.currentTimeMillis();
		try {
			if (isPartitioningBehaviorEnabled()) {
				HQLBean hqlBean = null;
				String method = ".search";
				if (isHQL) {
					hqlBean = getHQLBeanByHql(hql);

				} else {
					hqlBean = getHQLBeanBySql(hql);
					method = ".createSQLQuery";
				}
				Object entity = hqlBean2Entity(hqlBean,
						getParameters(hql, null), args, true, true);
				SortedMap<String, DataSource> dsMap = lookupDataSourcesByRouter(
						hqlBean.getClassName() + method, entity);
				if (logger.isDebugEnabled())
					logger.debug("dsMap size is : " + dsMap.size());
				if (!MapUtils.isEmpty(dsMap)) {
					List<Object> originalResultList = executeInConcurrency(
							action, dsMap);
					if (MapUtils.isNotEmpty(getMergers())
							&& getMergers().containsKey(
									hqlBean.getClassName() + method)) {
						IMerger<Object, Object> merger = getMergers().get(
								hqlBean.getClassName() + method);
						if (merger != null) {
							return (List) merger.merge(originalResultList);
						}
					}

					List<Object> resultList = new ArrayList<Object>();
					for (Object item : originalResultList) {
						resultList.addAll((List) item);
					}
					return resultList;
				}
			}
			return Collections.emptySet();
		} finally {
			if (isProfileLongTimeRunningSql()) {
				long interval = System.currentTimeMillis() - startTimestamp;
				if (interval > getLongTimeRunningSqlIntervalThreshold()) {
					logger.warn(
							"SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.",
							new Object[] { hql, args.values(), interval });
				}
			}
		}
	}

	/**
	 * @description
	 * @param action
	 * @param hql
	 * @return
	 */
	public Object executeWithUpdateHQL(final HibernateCallback action,
			final String hql, final boolean isHQL, final HashMap args) {
		long startTimestamp = System.currentTimeMillis();
		try {
			if (isPartitioningBehaviorEnabled()) {
				HQLBean hqlBean = null;
				String method = ".bupdate";
				if (isHQL) {
					hqlBean = getHQLBeanByHql(hql);

				} else {
					hqlBean = getHQLBeanBySql(hql);
					method = ".updatenative";
				}
				Object entity = hqlBean2Entity(hqlBean,
						getParameters(hql, null), args, isHQL, true);
				SortedMap<String, DataSource> dsMap = lookupDataSourcesByRouter(
						hqlBean.getClassName() + method, entity);
				if (logger.isDebugEnabled())
					logger.debug("dsMap size is : " + dsMap.size());
				if (!MapUtils.isEmpty(dsMap)) {
					if (dsMap.size() == 1) {
						DataSource dataSource = dsMap.get(dsMap.firstKey());
						return executeWith(dataSource, action);
					} else {
						logger.error("CobarHibernateTemplate executeWithUpdateHQL matched more than one Datasource!");
						List<Object> result = executeInConcurrency(action,
								dsMap);
						return result.isEmpty() ? 0 : result.get(0);
					}
				} else {
					logger.error("CobarHibernateTemplate executeWithUpdateHQL no matched any Datasource!");
				}
			}
			return 0;
		} finally {
			if (isProfileLongTimeRunningSql()) {
				long interval = System.currentTimeMillis() - startTimestamp;
				if (interval > getLongTimeRunningSqlIntervalThreshold()) {
					logger.warn(
							"SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.",
							new Object[] { hql, args.values(), interval });
				}
			}
		}
	}

	protected Object executeWith(DataSource dataSource, HibernateCallback action)
			throws DataAccessException {
		return executeWith(dataSource, action, false, true);
	}

	protected Object executeWith(DataSource dataSource,
			HibernateCallback action, boolean enforceNewSession,
			boolean enforceNativeSession) throws DataAccessException {

		Assert.notNull(action, "Callback object must not be null");
		Session session = null;
		boolean existingTransaction = true;
		FlushMode previousFlushMode = null;
		try {
			Connection springCon = null;
			boolean transactionAware = (dataSource instanceof TransactionAwareDataSourceProxy);

			// Obtain JDBC Connection to operate on...
			try {
				if (logger.isDebugEnabled())
					logger.debug("Current Datasource Connection Url is : "
							+ dataSource.getConnection().getMetaData().getURL());
				springCon = (transactionAware ? dataSource.getConnection()
						: DataSourceUtils.doGetConnection(dataSource));
			} catch (SQLException ex) {
				throw new CannotGetJdbcConnectionException(
						"Could not get JDBC Connection", ex);
			}
			session = getSessionFactory().openSession(springCon);
			// getSessionFactory().getCurrentSession().reconnect(connection);
			existingTransaction = (!enforceNewSession && (!isAllowCreate() || SessionFactoryUtils
					.isSessionTransactional(session, getSessionFactory())));
			if (existingTransaction) {
				logger.debug("Found thread-bound Session for HibernateTemplate");
			}
			try {
				previousFlushMode = applyFlushMode(session, existingTransaction);
				enableFilters(session);
				Session sessionToExpose = (enforceNativeSession
						|| isExposeNativeSession() ? session
						: createSessionProxy(session));
				Object result = action.doInHibernate(sessionToExpose);
				flushIfNecessary(session, existingTransaction);
				return result;
			} catch (HibernateException ex) {
				throw convertHibernateAccessException(ex);
			} catch (SQLException ex) {
				throw convertJdbcAccessException(ex);
			} catch (RuntimeException ex) {
				throw ex;
			} finally {
				try {
					if (springCon != null) {
						if (transactionAware) {
							springCon.close();
						} else {
							DataSourceUtils.doReleaseConnection(springCon,
									dataSource);
						}
					}
				} catch (Throwable ex) {
					logger.debug("Could not close JDBC Connection", ex);
				}
			}
		} finally {
			if (existingTransaction) {
				logger.debug("Not closing pre-bound Hibernate Session after HibernateTemplate");
				disableFilters(session);
				if (previousFlushMode != null) {
					session.setFlushMode(previousFlushMode);
				}
			} else {
				// Never use deferred close for an explicitly new Session.
				if (isAlwaysUseNewSession()) {
					SessionFactoryUtils.closeSession(session);
				} else {
					SessionFactoryUtils.releaseSession(session,
							getSessionFactory());
				}
			}
		}
	}

	public List<Object> executeSQLInConcurrency(String sql, Map mapParam,
			SortedMap<String, DataSource> dsMap) {
		List<ConcurrentRequestForSQL> requests = new ArrayList<ConcurrentRequestForSQL>();
		for (Map.Entry<String, DataSource> entry : dsMap.entrySet()) {
			ConcurrentRequestForSQL request = new ConcurrentRequestForSQL();
			request.setSql(sql);
			request.setParam(mapParam);
			request.setDataSource(entry.getValue());
			request.setExecutor(getDataSourceSpecificExecutors().get(
					entry.getKey()));
			requests.add(request);
		}
		return getConcurrentRequestProcessor().processQuerySQL(requests);
	}

	public List<Object> executeInConcurrency(HibernateCallback action,
			SortedMap<String, DataSource> dsMap) {
		List<ConcurrentRequest> requests = new ArrayList<ConcurrentRequest>();
		for (Map.Entry<String, DataSource> entry : dsMap.entrySet()) {
			ConcurrentRequest request = new ConcurrentRequest();
			request.setAction(action);
			request.setDataSource(entry.getValue());
			request.setExecutor(getDataSourceSpecificExecutors().get(
					entry.getKey()));
			requests.add(request);
		}
		return getConcurrentRequestProcessor().process(requests);
	}

	@Override
	public Object get(final Class entityClass, final Serializable id) {
		return this.get(entityClass, id, null);
	}

	@Override
	public Object get(final Class entityClass, final Serializable id,
			final LockMode lockMode) throws DataAccessException {
		long startTimestamp = System.currentTimeMillis();
		try {
			if (isPartitioningBehaviorEnabled()) {
				SortedMap<String, DataSource> dsMap = lookupDataSourcesByRouter(
						entityClass.getName() + ".get",
						serializable2Entity(entityClass, id));
				if (!MapUtils.isEmpty(dsMap)) {
					HibernateCallback action = new HibernateCallback() {
						public Object doInHibernate(Session session)
								throws HibernateException {
							if (lockMode != null) {
								return session.get(entityClass, id, lockMode);
							} else {
								return session.get(entityClass, id);
							}
						}
					};
					List<Object> resultList = executeInConcurrency(action,
							dsMap);
					@SuppressWarnings("unchecked")
					Collection<Object> filteredResultList = CollectionUtils
							.select(resultList, new Predicate() {
								public boolean evaluate(Object item) {
									return item != null;
								}
							});
					if (filteredResultList.size() > 1) {
						throw new IncorrectResultSizeDataAccessException(1);
					}
					if (CollectionUtils.isEmpty(filteredResultList)) {
						return null;
					}
					return filteredResultList.iterator().next();
				}
			}
			return super.get(entityClass, id, lockMode);
		} finally {
			if (isProfileLongTimeRunningSql()) {
				long interval = System.currentTimeMillis() - startTimestamp;
				if (interval > getLongTimeRunningSqlIntervalThreshold()) {
					logger.warn(
							"SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.",
							new Object[] { entityClass, id, interval });
				}
			}
		}
	}

	@Override
	public Object get(final String entityName, final Serializable id) {
		return this.get(entityName, id, null);
	}

	@Override
	public Object get(final String entityName, final Serializable id,
			final LockMode lockMode) throws DataAccessException {
		long startTimestamp = System.currentTimeMillis();
		try {
			if (isPartitioningBehaviorEnabled()) {
				SortedMap<String, DataSource> dsMap = lookupDataSourcesByRouter(
						entityName + ".get",
						serializable2Entity(entityName, id));
				if (!MapUtils.isEmpty(dsMap)) {
					HibernateCallback action = new HibernateCallback() {
						public Object doInHibernate(Session session)
								throws HibernateException {
							if (lockMode != null) {
								return session.get(entityName, id, lockMode);
							} else {
								return session.get(entityName, id);
							}
						}
					};
					List<Object> resultList = executeInConcurrency(action,
							dsMap);
					@SuppressWarnings("unchecked")
					Collection<Object> filteredResultList = CollectionUtils
							.select(resultList, new Predicate() {
								public boolean evaluate(Object item) {
									return item != null;
								}
							});
					if (filteredResultList.size() > 1) {
						throw new IncorrectResultSizeDataAccessException(1);
					}
					if (CollectionUtils.isEmpty(filteredResultList)) {
						return null;
					}
					return filteredResultList.iterator().next();
				}
			}
			return super.get(entityName, id, lockMode);
		} finally {
			if (isProfileLongTimeRunningSql()) {
				long interval = System.currentTimeMillis() - startTimestamp;
				if (interval > getLongTimeRunningSqlIntervalThreshold()) {
					logger.warn(
							"SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.",
							new Object[] { entityName, id, interval });
				}
			}
		}
	}

	@Override
	public Object load(final Class entityClass, final Serializable id) {
		return this.load(entityClass, id, null);
	}

	@Override
	public Object load(final Class entityClass, final Serializable id,
			final LockMode lockMode) throws DataAccessException {
		long startTimestamp = System.currentTimeMillis();
		try {
			if (isPartitioningBehaviorEnabled()) {
				SortedMap<String, DataSource> dsMap = lookupDataSourcesByRouter(
						entityClass.getName() + ".load",
						serializable2Entity(entityClass, id));
				if (!MapUtils.isEmpty(dsMap)) {
					HibernateCallback action = new HibernateCallback() {
						public Object doInHibernate(Session session)
								throws HibernateException {
							if (lockMode != null) {
								return session.load(entityClass, id, lockMode);
							} else {
								return session.load(entityClass, id);
							}
						}
					};
					List<Object> resultList = executeInConcurrency(action,
							dsMap);
					@SuppressWarnings("unchecked")
					Collection<Object> filteredResultList = CollectionUtils
							.select(resultList, new Predicate() {
								public boolean evaluate(Object item) {
									return item != null;
								}
							});
					if (filteredResultList.size() > 1) {
						throw new IncorrectResultSizeDataAccessException(1);
					}
					if (CollectionUtils.isEmpty(filteredResultList)) {
						return null;
					}
					return filteredResultList.iterator().next();
				}
			}
			return super.load(entityClass, id, lockMode);
		} finally {
			if (isProfileLongTimeRunningSql()) {
				long interval = System.currentTimeMillis() - startTimestamp;
				if (interval > getLongTimeRunningSqlIntervalThreshold()) {
					logger.warn(
							"SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.",
							new Object[] { entityClass, id, interval });
				}
			}
		}
	}

	@Override
	public Object load(final String entityName, final Serializable id) {
		return this.load(entityName, id, null);
	}

	@Override
	public Object load(final String entityName, final Serializable id,
			final LockMode lockMode) throws DataAccessException {

		long startTimestamp = System.currentTimeMillis();
		try {
			if (isPartitioningBehaviorEnabled()) {
				SortedMap<String, DataSource> dsMap = lookupDataSourcesByRouter(
						entityName + ".load",
						serializable2Entity(entityName, id));
				if (!MapUtils.isEmpty(dsMap)) {
					HibernateCallback action = new HibernateCallback() {
						public Object doInHibernate(Session session)
								throws HibernateException {
							if (lockMode != null) {
								return session.load(entityName, id, lockMode);
							} else {
								return session.load(entityName, id);
							}
						}
					};
					List<Object> resultList = executeInConcurrency(action,
							dsMap);
					@SuppressWarnings("unchecked")
					Collection<Object> filteredResultList = CollectionUtils
							.select(resultList, new Predicate() {
								public boolean evaluate(Object item) {
									return item != null;
								}
							});
					if (filteredResultList.size() > 1) {
						throw new IncorrectResultSizeDataAccessException(1);
					}
					if (CollectionUtils.isEmpty(filteredResultList)) {
						return null;
					}
					return filteredResultList.iterator().next();
				}
			}
			return super.load(entityName, id, lockMode);
		} finally {
			if (isProfileLongTimeRunningSql()) {
				long interval = System.currentTimeMillis() - startTimestamp;
				if (interval > getLongTimeRunningSqlIntervalThreshold()) {
					logger.warn(
							"SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.",
							new Object[] { entityName, id, interval });
				}
			}
		}
	}

	@Override
	public List find(final String queryString, final Object[] values)
			throws DataAccessException {
		auditSqlIfNecessary(queryString, values);
		long startTimestamp = System.currentTimeMillis();
		try {
			if (isPartitioningBehaviorEnabled()) {
				HQLBean hqlBean = getHQLBeanByHql(queryString);
				Object entity = hqlBean2Entity(hqlBean,
						getParameters(hqlBean.getSql(), null), values);
				SortedMap<String, DataSource> dsMap = lookupDataSourcesByRouter(
						hqlBean.getClassName() + ".find", entity);
				if (logger.isDebugEnabled())
					logger.debug("dsMap size is : " + dsMap.size());
				if (!MapUtils.isEmpty(dsMap)) {
					HibernateCallback action = new HibernateCallback() {
						public Object doInHibernate(Session session)
								throws HibernateException, SQLException {
							Query queryObject = session
									.createQuery(queryString);
							prepareQuery(queryObject);
							if (values != null) {
								for (int i = 0; i < values.length; i++) {
									queryObject.setParameter(i, values[i]);
								}
							}
							return queryObject.list();
						}
					};

					List<Object> originalResultList = executeInConcurrency(
							action, dsMap);
					if (MapUtils.isNotEmpty(getMergers())
							&& getMergers().containsKey(
									hqlBean.getClassName() + ".find")) {
						IMerger<Object, Object> merger = getMergers().get(
								hqlBean.getClassName() + ".find");
						if (merger != null) {
							return (List) merger.merge(originalResultList);
						}
					}

					List<Object> resultList = new ArrayList<Object>();
					for (Object item : originalResultList) {
						resultList.addAll((List) item);
					}
					return resultList;
				}
			}
			return super.find(queryString, values);
		} finally {
			if (isProfileLongTimeRunningSql()) {
				long interval = System.currentTimeMillis() - startTimestamp;
				if (interval > getLongTimeRunningSqlIntervalThreshold()) {
					logger.warn(
							"SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.",
							new Object[] { queryString, values, interval });
				}
			}
		}
	}

	@Override
	public Serializable save(final Object entity) throws DataAccessException {
		long startTimestamp = System.currentTimeMillis();
		try {
			if (isPartitioningBehaviorEnabled()) {
				SortedMap<String, DataSource> dsMap = lookupDataSourcesByRouter(
						entity.getClass().getName() + ".save", entity);
				if (!MapUtils.isEmpty(dsMap)) {
					HibernateCallback action = new HibernateCallback() {
						public Object doInHibernate(Session session)
								throws HibernateException, SQLException {
							checkWriteOperationAllowed(session);
							return session.save(entity);
						}
					};
					if (dsMap.size() == 1) {
						DataSource dataSource = dsMap.get(dsMap.firstKey());
						return (Serializable) executeWith(dataSource, action);
					} else {
						throw new IllegalArgumentException(
								"'hibernate.save' can not insert data to more than one database");
						// executeInConcurrency(action , dsMap) ;
					}
				}
			}
			return super.save(entity);
		} finally {
			if (isProfileLongTimeRunningSql()) {
				long interval = System.currentTimeMillis() - startTimestamp;
				if (interval > getLongTimeRunningSqlIntervalThreshold()) {
					logger.warn(
							"SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.",
							new Object[] { entity, "", interval });
				}
			}
		}
	}

	@Override
	public Serializable save(final String entityName, final Object entity)
			throws DataAccessException {
		long startTimestamp = System.currentTimeMillis();
		try {
			if (isPartitioningBehaviorEnabled()) {
				SortedMap<String, DataSource> dsMap = lookupDataSourcesByRouter(
						entity.getClass().getName() + ".save", entity);
				if (!MapUtils.isEmpty(dsMap)) {
					HibernateCallback action = new HibernateCallback() {
						public Object doInHibernate(Session session)
								throws HibernateException, SQLException {
							checkWriteOperationAllowed(session);
							return session.save(entityName, entity);
						}
					};
					if (dsMap.size() == 1) {
						DataSource dataSource = dsMap.get(dsMap.firstKey());
						return (Serializable) executeWith(dataSource, action);
					} else {
						throw new IllegalArgumentException(
								"'hibernate.save' can not insert data to more than one database");
						// executeInConcurrency(action , dsMap) ;
					}
				}
			}
			return super.save(entityName, entity);
		} finally {
			if (isProfileLongTimeRunningSql()) {
				long interval = System.currentTimeMillis() - startTimestamp;
				if (interval > getLongTimeRunningSqlIntervalThreshold()) {
					logger.warn(
							"SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.",
							new Object[] { entity, entityName, interval });
				}
			}
		}
	}

	@Override
	public void update(final Object entity, final LockMode lockMode)
			throws DataAccessException {
		long startTimestamp = System.currentTimeMillis();
		try {
			if (isPartitioningBehaviorEnabled()) {
				SortedMap<String, DataSource> dsMap = lookupDataSourcesByRouter(
						entity.getClass().getName() + ".update", entity);
				if (!MapUtils.isEmpty(dsMap)) {
					HibernateCallback action = new HibernateCallback() {
						public Object doInHibernate(Session session)
								throws HibernateException, SQLException {
							checkWriteOperationAllowed(session);
							session.update(entity);
							if (lockMode != null) {
								session.lock(entity, lockMode);
							}
							return null;
						}
					};
					if (dsMap.size() == 1) {
						DataSource dataSource = dsMap.get(dsMap.firstKey());
						executeWith(dataSource, action);
					} else {
						executeInConcurrency(action, dsMap);
					}
				} else
					super.update(entity, lockMode);
			} else
				super.update(entity, lockMode);
		} finally {
			if (isProfileLongTimeRunningSql()) {
				long interval = System.currentTimeMillis() - startTimestamp;
				if (interval > getLongTimeRunningSqlIntervalThreshold()) {
					logger.warn(
							"SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.",
							new Object[] { entity, "", interval });
				}
			}
		}
	}

	@Override
	public void update(final String entityName, final Object entity,
			final LockMode lockMode) throws DataAccessException {
		long startTimestamp = System.currentTimeMillis();
		try {
			if (isPartitioningBehaviorEnabled()) {
				SortedMap<String, DataSource> dsMap = lookupDataSourcesByRouter(
						entity.getClass().getName() + ".update", entity);
				if (!MapUtils.isEmpty(dsMap)) {
					HibernateCallback action = new HibernateCallback() {

						public Object doInHibernate(Session session)
								throws HibernateException, SQLException {
							checkWriteOperationAllowed(session);
							session.update(entityName, entity);
							if (lockMode != null) {
								session.lock(entity, lockMode);
							}
							return null;
						}
					};
					if (dsMap.size() == 1) {
						DataSource dataSource = dsMap.get(dsMap.firstKey());
						executeWith(dataSource, action);
					} else {
						executeInConcurrency(action, dsMap);
					}
				} else
					super.update(entityName, entity, lockMode);
			} else
				super.update(entityName, entity, lockMode);
		} finally {
			if (isProfileLongTimeRunningSql()) {
				long interval = System.currentTimeMillis() - startTimestamp;
				if (interval > getLongTimeRunningSqlIntervalThreshold()) {
					logger.warn(
							"SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.",
							new Object[] { entity, entityName, interval });
				}
			}
		}
	}

	@Override
	public void saveOrUpdate(final Object entity) throws DataAccessException {
		long startTimestamp = System.currentTimeMillis();
		try {
			if (isPartitioningBehaviorEnabled()) {
				SortedMap<String, DataSource> dsMap = lookupDataSourcesByRouter(
						entity.getClass().getName() + ".saveOrUpdate", entity);
				if (!MapUtils.isEmpty(dsMap)) {
					HibernateCallback action = new HibernateCallback() {

						public Object doInHibernate(Session session)
								throws HibernateException, SQLException {
							checkWriteOperationAllowed(session);
							session.saveOrUpdate(entity);
							return null;
						}
					};
					if (dsMap.size() == 1) {
						DataSource dataSource = dsMap.get(dsMap.firstKey());
						executeWith(dataSource, action);
					} else {
						executeInConcurrency(action, dsMap);
					}
				} else
					super.saveOrUpdate(entity);
			} else
				super.saveOrUpdate(entity);
		} finally {
			if (isProfileLongTimeRunningSql()) {
				long interval = System.currentTimeMillis() - startTimestamp;
				if (interval > getLongTimeRunningSqlIntervalThreshold()) {
					logger.warn(
							"SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.",
							new Object[] { entity, "", interval });
				}
			}
		}
	}

	@Override
	public void saveOrUpdate(final String entityName, final Object entity)
			throws DataAccessException {
		long startTimestamp = System.currentTimeMillis();
		try {
			if (isPartitioningBehaviorEnabled()) {
				SortedMap<String, DataSource> dsMap = lookupDataSourcesByRouter(
						entity.getClass().getName() + ".saveOrUpdate", entity);
				if (!MapUtils.isEmpty(dsMap)) {
					HibernateCallback action = new HibernateCallback() {
						public Object doInHibernate(Session session)
								throws HibernateException, SQLException {
							checkWriteOperationAllowed(session);
							session.saveOrUpdate(entityName, entity);
							return null;
						}
					};
					if (dsMap.size() == 1) {
						DataSource dataSource = dsMap.get(dsMap.firstKey());
						executeWith(dataSource, action);
					} else {
						executeInConcurrency(action, dsMap);
					}
				} else
					super.saveOrUpdate(entityName, entity);
			} else
				super.saveOrUpdate(entityName, entity);
		} finally {
			if (isProfileLongTimeRunningSql()) {
				long interval = System.currentTimeMillis() - startTimestamp;
				if (interval > getLongTimeRunningSqlIntervalThreshold()) {
					logger.warn(
							"SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.",
							new Object[] { entity, entityName, interval });
				}
			}
		}
	}

	@Override
	public void saveOrUpdateAll(final Collection entities)
			throws DataAccessException {
		if (!entities.isEmpty()) {
			long startTimestamp = System.currentTimeMillis();
			try {
				batchReordering(entities, "saveOrUpdateAll");
			} finally {
				if (isProfileLongTimeRunningSql()) {
					long interval = System.currentTimeMillis() - startTimestamp;
					if (interval > getLongTimeRunningSqlIntervalThreshold()) {
						logger.warn(
								"SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.",
								new Object[] { "saveOrUpdateAll", "entities",
										interval });
					}
				}
			}
		}
	}

	private Object batchReordering(final Collection entities,
			final String methodName) {
		Set<String> keys = new HashSet<String>();
		keys.add(getDefaultDataSourceName());
		keys.addAll(getCobarDataSourceService().getDataSources().keySet());
		final CobarMRBase mrbase = new CobarMRBase(keys);
		ExecutorService executor = createCustomExecutorService(Runtime
				.getRuntime().availableProcessors(),
				"batchInsertAfterReordering");
		try {
			final StringBuffer exceptionStaktrace = new StringBuffer();
			Collection<?> paramCollection = entities;
			final CountDownLatch latch = new CountDownLatch(
					paramCollection.size());
			Iterator<?> iter = paramCollection.iterator();
			while (iter.hasNext()) {
				final Object entity = iter.next();
				Runnable task = new Runnable() {
					public void run() {
						try {
							SortedMap<String, DataSource> dsMap = lookupDataSourcesByRouter(
									entities.iterator().next().getClass()
											.getName()
											+ "." + methodName, entity);
							if (MapUtils.isEmpty(dsMap)) {
								logger.info(
										"can't find routing rule for {} with parameter {}, so use default data source for it.",
										entities.iterator().next().getClass()
												.getName()
												+ "." + methodName, entity);
								mrbase.emit(getDefaultDataSourceName(), entity);
							} else {
								if (dsMap.size() > 1) {
									throw new IllegalArgumentException(
											"unexpected routing result, found more than 1 target data source for current entity:"
													+ entity);
								}
								mrbase.emit(dsMap.firstKey(), entity);
							}
						} catch (Throwable t) {
							exceptionStaktrace.append(ExceptionUtils
									.getFullStackTrace(t));
						} finally {
							latch.countDown();
						}
					}
				};
				executor.execute(task);
			}
			try {
				latch.await();
			} catch (InterruptedException e) {
				throw new ConcurrencyFailureException(
						"unexpected interruption when re-arranging parameter collection into sub-collections ",
						e);
			}

			if (exceptionStaktrace.length() > 0) {
				throw new ConcurrencyFailureException(
						"unpected exception when re-arranging parameter collection, check previous log for details.\n"
								+ exceptionStaktrace);
			}
		} finally {
			executor.shutdown();
		}

		List<ConcurrentRequest> requests = new ArrayList<ConcurrentRequest>();

		for (final Map.Entry<String, List<Object>> entity : mrbase
				.getResources().entrySet()) {
			final List<Object> paramList = entity.getValue();
			if (CollectionUtils.isEmpty(paramList)) {
				continue;
			}

			String identity = entity.getKey();

			final DataSource dataSourceToUse = findDataSourceToUse(identity);

			HibernateCallback action = null;
			if (methodName.equals(SAVEORUPDATEALL_METHOD_NAME)) {
				action = new HibernateCallback() {
					public Object doInHibernate(Session session)
							throws HibernateException, SQLException {
						checkWriteOperationAllowed(session);
						for (Object obj : paramList) {
							session.saveOrUpdate(obj);
						}
						return null;
					}
				};
			} else if (methodName.equals(DELETEALL_METHOD_NAME)) {
				action = new HibernateCallback() {
					public Object doInHibernate(Session session)
							throws HibernateException {
						checkWriteOperationAllowed(session);
						for (Object obj : paramList) {
							session.delete(obj);
						}
						return null;
					}
				};
			} else {
				throw new IllegalArgumentException(
						"'batchReordering' error methodMethod , must be deleteAll or saveOrUpdateAll");
			}

			ConcurrentRequest request = new ConcurrentRequest();
			request.setDataSource(dataSourceToUse);
			request.setAction(action);
			request.setExecutor(getDataSourceSpecificExecutors().get(identity));
			requests.add(request);
		}
		return getConcurrentRequestProcessor().process(requests);
	}

	private DataSource findDataSourceToUse(String key) {
		DataSource dataSourceToUse = null;
		if (StringUtils.equals(key, getDefaultDataSourceName())) {
			dataSourceToUse = SessionFactoryUtils
					.getDataSource(getSessionFactory());
		} else {
			dataSourceToUse = getCobarDataSourceService().getDataSources().get(
					key);
		}
		return dataSourceToUse;
	}

	@Override
	public void delete(final Object entity, final LockMode lockMode)
			throws DataAccessException {
		long startTimestamp = System.currentTimeMillis();
		try {
			if (isPartitioningBehaviorEnabled()) {
				SortedMap<String, DataSource> dsMap = lookupDataSourcesByRouter(
						entity.getClass().getName() + ".delete", entity);
				if (!MapUtils.isEmpty(dsMap)) {
					HibernateCallback action = new HibernateCallback() {
						public Object doInHibernate(Session session)
								throws HibernateException {
							checkWriteOperationAllowed(session);
							if (lockMode != null) {
								session.lock(entity, lockMode);
							}
							session.delete(entity);
							return null;
						}
					};
					if (dsMap.size() == 1) {
						DataSource dataSource = dsMap.get(dsMap.firstKey());
						executeWith(dataSource, action);
					} else {
						executeInConcurrency(action, dsMap);
					}
				} else
					super.delete(entity, lockMode);
			} else
				super.delete(entity, lockMode);
		} finally {
			if (isProfileLongTimeRunningSql()) {
				long interval = System.currentTimeMillis() - startTimestamp;
				if (interval > getLongTimeRunningSqlIntervalThreshold()) {
					logger.warn(
							"SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.",
							new Object[] { entity, "", interval });
				}
			}
		}
	}

	@Override
	public void delete(final String entityName, final Object entity,
			final LockMode lockMode) throws DataAccessException {
		long startTimestamp = System.currentTimeMillis();
		try {
			if (isPartitioningBehaviorEnabled()) {
				SortedMap<String, DataSource> dsMap = lookupDataSourcesByRouter(
						entity.getClass().getName() + ".delete", entity);
				if (!MapUtils.isEmpty(dsMap)) {
					HibernateCallback action = new HibernateCallback() {

						public Object doInHibernate(Session session)
								throws HibernateException, SQLException {
							checkWriteOperationAllowed(session);
							if (lockMode != null) {
								session.lock(entityName, entity, lockMode);
							}
							session.delete(entityName, entity);
							return null;
						}
					};

					if (dsMap.size() == 1) {
						DataSource dataSource = dsMap.get(dsMap.firstKey());
						executeWith(dataSource, action);
					} else {
						executeInConcurrency(action, dsMap);
					}
				} else
					super.delete(entityName, entity, lockMode);
			} else
				super.delete(entityName, entity, lockMode);
		} finally {
			if (isProfileLongTimeRunningSql()) {
				long interval = System.currentTimeMillis() - startTimestamp;
				if (interval > getLongTimeRunningSqlIntervalThreshold()) {
					logger.warn(
							"SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.",
							new Object[] { entity, entityName, interval });
				}
			}
		}
	}

	@Override
	public void deleteAll(final Collection entities) throws DataAccessException {
		if (!entities.isEmpty()) {
			long startTimestamp = System.currentTimeMillis();
			try {
				batchReordering(entities, "deleteAll");
			} finally {
				if (isProfileLongTimeRunningSql()) {
					long interval = System.currentTimeMillis() - startTimestamp;
					if (interval > getLongTimeRunningSqlIntervalThreshold()) {
						logger.warn(
								"SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.",
								new Object[] { "deleteAll", entities, interval });
					}
				}
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.orm.hibernate3.HibernateTemplate#evict(java.lang.Object)
	 * 
	 * @todo
	 */
	@Override
	public void evict(final Object entity) throws DataAccessException {
		long startTimestamp = System.currentTimeMillis();
		try {
			if (isPartitioningBehaviorEnabled()) {
				SortedMap<String, DataSource> dsMap = lookupDataSourcesByRouter(
						entity.getClass().getName() + ".evict", entity);
				if (!MapUtils.isEmpty(dsMap)) {
					HibernateCallback action = new HibernateCallback() {
						public Object doInHibernate(Session session)
								throws HibernateException {
							session.evict(entity);
							return null;
						}
					};
					if (dsMap.size() == 1) {
						DataSource dataSource = dsMap.get(dsMap.firstKey());
						executeWith(dataSource, action);
					} else {
						executeInConcurrency(action, dsMap);
					}
				} else
					super.evict(entity);
			} else
				super.evict(entity);
		} finally {
			if (isProfileLongTimeRunningSql()) {
				long interval = System.currentTimeMillis() - startTimestamp;
				if (interval > getLongTimeRunningSqlIntervalThreshold()) {
					logger.warn(
							"SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.",
							new Object[] { "evict", entity, interval });
				}
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.orm.hibernate3.HibernateTemplate#findByNamedQuery(java.lang.String, java.lang.Object[])
	 * 
	 * @todo
	 */
	@Override
	public List findByNamedQuery(final String queryName, final Object[] values)
			throws DataAccessException {
		throw new IllegalAccessError("error invoke method");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.orm.hibernate3.HibernateTemplate#merge(java.lang.Object)
	 * 
	 * @todo
	 */
	@Override
	public Object merge(final Object entity) throws DataAccessException {
		long startTimestamp = System.currentTimeMillis();
		try {
			if (isPartitioningBehaviorEnabled()) {
				SortedMap<String, DataSource> dsMap = lookupDataSourcesByRouter(
						entity.getClass().getName() + ".merge", entity);
				if (!MapUtils.isEmpty(dsMap)) {
					HibernateCallback action = new HibernateCallback() {
						public Object doInHibernate(Session session)
								throws HibernateException {
							checkWriteOperationAllowed(session);
							return session.merge(entity);
						}
					};
					if (dsMap.size() == 1) {
						DataSource dataSource = dsMap.get(dsMap.firstKey());
						return executeWith(dataSource, action);
					} else {
						throw new IllegalArgumentException(
								"'hibernate.merge' can not merge data to more than one database");
					}
				}
			}
			return super.merge(entity);
		} finally {
			if (isProfileLongTimeRunningSql()) {
				long interval = System.currentTimeMillis() - startTimestamp;
				if (interval > getLongTimeRunningSqlIntervalThreshold()) {
					logger.warn(
							"SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.",
							new Object[] { "merge", entity, interval });
				}
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.orm.hibernate3.HibernateTemplate#merge(java.lang.String, java.lang.Object)
	 * 
	 * @todo
	 */
	@Override
	public Object merge(final String entityName, final Object entity)
			throws DataAccessException {
		long startTimestamp = System.currentTimeMillis();
		try {
			if (isPartitioningBehaviorEnabled()) {
				SortedMap<String, DataSource> dsMap = lookupDataSourcesByRouter(
						entity.getClass().getName() + ".merge", entity);
				if (!MapUtils.isEmpty(dsMap)) {
					HibernateCallback action = new HibernateCallback() {
						public Object doInHibernate(Session session)
								throws HibernateException {
							checkWriteOperationAllowed(session);
							return session.merge(entityName, entity);
						}
					};
					if (dsMap.size() == 1) {
						DataSource dataSource = dsMap.get(dsMap.firstKey());
						return executeWith(dataSource, action);
					} else {
						throw new IllegalArgumentException(
								"'hibernate.merge' can not merge data to more than one database");
					}
				}
			}
			return super.merge(entityName, entity);
		} finally {
			if (isProfileLongTimeRunningSql()) {
				long interval = System.currentTimeMillis() - startTimestamp;
				if (interval > getLongTimeRunningSqlIntervalThreshold()) {
					logger.warn(
							"SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.",
							new Object[] { "merge", entity, interval });
				}
			}
		}
	}

	public Object serializable2Entity(Class entityClass, Serializable id) {
		Object entity = null;
		try {
			entity = entityClass.newInstance();
			SessionImplementor session = (SessionImplementor) getSession();
			EntityPersister p = session.getEntityPersister(
					entityClass.getName(), entity);
			p.setIdentifier(entity, id, session.getEntityMode());
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return entity;
	}

	public Object serializable2Entity(String entityName, Serializable id) {
		Object entity = null;
		try {
			entity = Class.forName(entityName).newInstance();
			SessionImplementor session = (SessionImplementor) getSession();
			EntityPersister p = session.getEntityPersister(entityName, entity);
			p.setIdentifier(entity, id, session.getEntityMode());
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return entity;
	}

	protected void auditSqlIfNecessary(final String hqlStr,
			final Object parameterObject) {
		if (getSqlAuditor() != null) {
			getSqlAuditorExecutor().execute(new Runnable() {
				public void run() {
					getSqlAuditor().audit(hqlStr, getSqlByHql(hqlStr),
							parameterObject);
				}
			});
		}
	}

	public String getSqlByHql(final String hqlStr) {
		QueryTranslatorImpl queryTranslator = new QueryTranslatorImpl(hqlStr,
				hqlStr, Collections.EMPTY_MAP,
				(SessionFactoryImplementor) getSessionFactory());
		queryTranslator.compile(Collections.EMPTY_MAP, false);
		return queryTranslator.getSQLString();
	}

	public String getClassNameByHql(final String hqlStr) {
		QueryTranslatorImpl queryTranslator = new QueryTranslatorImpl(hqlStr,
				hqlStr, Collections.EMPTY_MAP,
				(SessionFactoryImplementor) getSessionFactory());
		queryTranslator.compile(Collections.EMPTY_MAP, false);
		return queryTranslator.getReturnTypes()[0].getReturnedClass().getName();
	}

	/**
	 * @todo 目前只考虑了存在Where条件情况，也就是忽略了insert语句
	 * @param sql
	 *            ，应该是hql，sql中的字段跟class无法完成匹配，存在bug
	 * @param regex
	 * @return
	 */
	public List<String> getParameters(String sql, String regex) {
		ArrayList lstParam = new ArrayList();
		if (regex == null)
			regex = REGEX_WHERE_CONDITION;
		Pattern pattern = Pattern.compile(regex, 2);
		Matcher matcher = pattern.matcher(sql);

		boolean result = matcher.find();
		while (result) {

			String matcherVal = matcher.group(0);
			if ((!":mi:ss".equals(matcherVal)) && (!":mi".equals(matcherVal))
					&& (!":ss".equals(matcherVal))) {
				lstParam.add(matcher.group(0));
			}
			result = matcher.find();
		}
		if (logger.isDebugEnabled())
			logger.debug("the sql param is : " + lstParam);
		return lstParam;
	}

	public HQLBean getHQLBeanByHql(final String hqlStr) {
		HQLBean hqlBean = new HQLBean();
		hqlBean.setHql(hqlStr);
		QueryTranslatorImpl queryTranslator = new QueryTranslatorImpl(hqlStr,
				hqlStr, Collections.EMPTY_MAP,
				(SessionFactoryImplementor) getSessionFactory());
		queryTranslator.compile(Collections.EMPTY_MAP, false);
		// hqlBean.setSql(queryTranslator.getSQLString());
		AST hqlAst = queryTranslator.getSqlAST().getWalker().getAST();

		SqlGenerator gen = new SqlGenerator(
				(SessionFactoryImplementor) getSessionFactory());
		try {
			gen.statement(hqlAst);
			hqlBean.setSql(gen.getSQL());
		} catch (RecognitionException e) {
			e.printStackTrace();
		}

		FromElement fe = null;
		if (hqlAst instanceof QueryNode) {
			fe = (FromElement) ((QueryNode) hqlAst).getFromClause()
					.getFirstChild();
			hqlBean.setSqlType("SELECT");
		} else if (hqlAst instanceof UpdateStatement) {
			fe = (FromElement) ((UpdateStatement) hqlAst).getFromClause()
					.getFirstChild();
			hqlBean.setSqlType("UPDATE");
		} else {
			fe = (FromElement) ((DeleteStatement) hqlAst).getFromClause()
					.getFirstChild();
			hqlBean.setSqlType("DELETE");
		}

		String simpleClassName = fe.getClassName();
		hqlBean.setClassAlias(fe.getClassAlias() == null ? simpleClassName : fe.getClassAlias());
		hqlBean.setTableAlias(fe.getTableAlias());

		if (logger.isDebugEnabled()) {
			ASTPrinter printer = new ASTPrinter(SqlTokenTypes.class);
			logger.debug(printer.showAsString(fe, "--- SQL AST ---"));
		}

		for (Entry<String, String> entry : tableAndClassMappers.entrySet()) {
			if(entry.getValue().contains(simpleClassName)){
				if(!entry.getValue().contains(CLASSNAME_SPLIT_STR)){
					hqlBean.setClassName(entry.getValue());
				} else{
					String[] classNames = entry.getValue().split(CLASSNAME_SPLIT_STR);
					for(String calssName: classNames){
						if (calssName.endsWith(simpleClassName)) {
							hqlBean.setClassName(calssName);
							break;
						}
					}
				}
				break ;
			} 
		}
		return hqlBean;
	}

	public HQLBean getHQLBeanBySql(final String sqlStr) {
		HQLBean hqlBean = new HQLBean();
		String tableName = SQLParserUtils.findTableName(sqlStr);
		//String className = tableAndClassMappers.get(tableName.toLowerCase());
		String classNames = tableAndClassMappers.get(tableName.toLowerCase());
		//默认取table名对应className的第一个类，可能存在问题，如果需要分库的类是后加载，则取错了类。
		
		String className = classNames  ;
		if(classNames !=null && classNames.contains(CLASSNAME_SPLIT_STR)){
			className = getSimilarityClassName(tableName, classNames) ;
		}
		
		//String className = classNames != null ? classNames.split(CLASSNAME_SPLIT_STR)[0] : null;
		hqlBean.setSql(sqlStr);
		hqlBean.setSqlType(SQLParserUtils.getSqlType(sqlStr));
		hqlBean.setClassName(className);
		hqlBean.setTableAlias(tableName);
		hqlBean.setClassAlias(className);
		return hqlBean;
	}
	
	private String getSimilarityClassName(String tableName, String classNames){
		float rationValue = 0L ;
		String[] classNameArray = classNames.split(CLASSNAME_SPLIT_STR);
		
		String clazzName = classNameArray[0];
		for(String className : classNameArray){
			float ration = SimilarityUtils.getSimilarityRatio(tableName.toLowerCase(), className.toLowerCase());
			if(ration > rationValue){
				rationValue = ration ;
				clazzName = className;
			}
		}
		return clazzName;
	}

	/**
	 * @todo 由于paramRegex是通过匹配=的条件，对于参数值按照Object[i]设置的方式存在bug
	 * @description 此方法只find调用，传入的paramRegex 是HQL条件，故可以直接将条件当作propertyName
	 * @param hqlBean
	 * @param paramRegex
	 * @param paramValue
	 * @return
	 */
	public Object hqlBean2Entity(HQLBean hqlBean, List<String> paramRegex,
			Object[] paramValue) {
		Object obj = null;
		try {
			obj = Class.forName(hqlBean.getClassName()).newInstance();
			PersistentClass pc = configuration.getClassMapping(hqlBean
					.getClassName());
			Property idProperty = pc.getIdentifierProperty();
			//当使用composite-id情况，直接返回所有属性为null的初始化对象。
			if(idProperty == null){
				return obj ;
			}
			/*
			 * 解析composite-id情况，待完善
			KeyValue o = pc.getIdentifier();
			if(o != null) {
				for(Iterator<org.hibernate.mapping.Value> iterator = o.getColumnIterator() ; iterator.hasNext() ;){
					org.hibernate.mapping.Column v = (org.hibernate.mapping.Column)iterator.next() ;
					System.out.println(v.getName());
				}
			}
			*/
			int i = 0;
			for (String condition : paramRegex) {
				i++;
				Iterator<Property> ip = pc.getPropertyIterator();
				String[] s = condition.split("=");
				for (int ca = 0; ca < s.length; ca++) {
					s[ca] = s[ca].trim();
				}
				if (s[0].startsWith(hqlBean.getTableAlias()) || s[0].startsWith(hqlBean.getClassAlias())
						&& "SELECT".equals(hqlBean.getSqlType())) {
					String columnName = StringUtils.substringAfterLast(s[0], ".");
					String propertyName = "";
					while (ip.hasNext()) {
						Property property = ip.next();
						Iterator<Column> columns2 = property
								.getColumnIterator();
						while (columns2.hasNext()) {
							String colunm2Name = columns2.next().getName();
							if (columnName.equals(colunm2Name)) {
								propertyName = property.getName();
								break;
							}
						}
						if (StringUtils.isNotEmpty(propertyName))
							break;
					}
					if (idProperty.isComposite())
						continue;
					if (StringUtils.isEmpty(propertyName) && idProperty != null)
						propertyName = idProperty.getName();
					PropertyDescriptor pd = new PropertyDescriptor(
							propertyName, obj.getClass());
					Method method = pd.getWriteMethod();
					method.invoke(obj, paramValue[i - 1]);
				}  else if ("SELECT".equals(hqlBean.getSqlType()) && s[0].indexOf(".") < 0) {
					//由于是HQL所以colunmName 等于propertyName
					String columnName = s[0] ;
					String propertyName = "";
					while (ip.hasNext()) {
						Property property = ip.next();
						Iterator<Column> columns2 = property.getColumnIterator();
						while (columns2.hasNext()) {
							String colunm2Name = columns2.next().getName();
							if (columnName.equals(colunm2Name)) {
								propertyName = property.getName();
								break;
							}
						}
						if (StringUtils.isNotEmpty(propertyName))
							break;
					}
					if (idProperty.isComposite())
						continue;
					if (StringUtils.isEmpty(propertyName) && idProperty != null)
						propertyName = idProperty.getName();
					
					PropertyDescriptor pd = new PropertyDescriptor(propertyName, obj.getClass());
					Method method = pd.getWriteMethod();
					
					method.invoke(obj, paramValue[i - 1]);
				} else if (!"SELECT".equals(hqlBean.getSqlType())) {
					String columnName = s[0];
					String propertyName = "";
					while (ip.hasNext()) {
						Property property = ip.next();
						Iterator<Column> columns2 = property.getColumnIterator();
						while (columns2.hasNext()) {
							String colunm2Name = columns2.next().getName();
							if (columnName.equals(colunm2Name)) {
								propertyName = property.getName();
								break;
							}
						}
						if (StringUtils.isNotEmpty(propertyName))
							break;
					}
					if (idProperty.isComposite())
						continue;
					if (StringUtils.isEmpty(propertyName) && idProperty != null)
						propertyName = idProperty.getName();
					PropertyDescriptor pd = new PropertyDescriptor(
							propertyName, obj.getClass());
					Method method = pd.getWriteMethod();
					method.invoke(obj, paramValue[i - 1]);
				} else {
					// select 表名与where条件的表名前缀不匹配
				}
			}
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IntrospectionException e) {
			e.printStackTrace();
		}
		return obj;
	}

	public Object hqlBean2Entity(HQLBean hqlBean, List<String> paramRegex,
			Map map, boolean isHQL, boolean isMap)  {
		Object obj = null;
		//无法处理public List createSQLQuery(final int firstRow, final int maxRows, final String sql, final List<String> lstDwColAliasName, final Class poClass)
		
		if (map == null && paramRegex.size() > 0) {
			throw new IllegalArgumentException(
					"If the paramRegex not empty , the param Map can not be null !");
		}
		try {
			obj = Class.forName(hqlBean.getClassName()).newInstance();
			PersistentClass pc = configuration.getClassMapping(hqlBean
					.getClassName());
			Property idProperty = pc.getIdentifierProperty();
			if(idProperty == null){
				return obj ;
			}
			int index = 0 ;
			for (String condition : paramRegex) {
				index ++ ;
				Iterator<Property> ip = pc.getPropertyIterator();
				String[] s = condition.split("=");
				for (int ca = 0; ca < s.length; ca++) {
					s[ca] = s[ca].trim();
				}
				if ((s[0].startsWith(hqlBean.getTableAlias()) || s[0].startsWith(hqlBean.getClassAlias()))
						&& "SELECT".equals(hqlBean.getSqlType())) {
					String propertyName = "";
					String columnName = "";
					if (isHQL) {
						if(s[0].indexOf(".") > 0) 
							propertyName = StringUtils.substringAfterLast(s[0], ".");
						else
							propertyName = s[0];
					}
					else {
						columnName = StringUtils.substringAfterLast(s[0], ".");
						while (ip.hasNext()) {
							Property property = ip.next();
							Iterator<Column> columns2 = property
									.getColumnIterator();
							while (columns2.hasNext()) {
								String colunm2Name = columns2.next().getName();
								if (columnName.equals(colunm2Name)) {
									propertyName = property.getName();
									break;
								}
							}
							if (StringUtils.isNotEmpty(propertyName))
								break;
						}
						if (idProperty.isComposite())
							continue;
						if (StringUtils.isEmpty(propertyName)
								&& idProperty != null)
							propertyName = idProperty.getName();
					}
					PropertyDescriptor pd = new PropertyDescriptor(
							propertyName, obj.getClass());
					Method method = pd.getWriteMethod();
					if(isMap){
						if (isHQL)
							method.invoke(obj, map.get(s[1].substring(1)));
						else//无法进入的分支
							method.invoke(obj, map.get(columnName));
					} else
						method.invoke(obj, map.get(String.valueOf(index - 1 )));
				} else if ("SELECT".equals(hqlBean.getSqlType()) && s[0].indexOf(".") < 0) {
					String columnName = s[0] ;
					String propertyName = "";
					if(!isHQL){
						while (ip.hasNext()) {
							Property property = ip.next();
							Iterator<Column> columns2 = property.getColumnIterator();
							while (columns2.hasNext()) {
								String colunm2Name = columns2.next().getName();
								if (columnName.equals(colunm2Name)) {
									propertyName = property.getName();
									break;
								}
							}
							if (StringUtils.isNotEmpty(propertyName))
								break;
						}
						if (idProperty.isComposite())
							continue;
						if (StringUtils.isEmpty(propertyName) && idProperty != null)
							propertyName = idProperty.getName();
					} else{
						propertyName = columnName ;
					}
					PropertyDescriptor pd = new PropertyDescriptor(propertyName, obj.getClass());
					Method method = pd.getWriteMethod();
					if(isMap)
						method.invoke(obj, map.get(s[1].substring(1)));
					else
						method.invoke(obj, map.get(String.valueOf(index - 1 )));
				} else if (!"SELECT".equals(hqlBean.getSqlType())) {
					String columnName = "";
					String propertyName = "";
					if (isHQL) {
						if(s[0].indexOf(".") > 0) 
							propertyName = StringUtils.substringAfterLast(s[0], ".");
						else
							propertyName = s[0];
					}	
					else {
						columnName = s[0];
						while (ip.hasNext()) {
							Property property = ip.next();
							Iterator<Column> columns2 = property
									.getColumnIterator();
							while (columns2.hasNext()) {
								String colunm2Name = columns2.next().getName();
								if (columnName.equals(colunm2Name)) {
									propertyName = property.getName();
									break;
								}
							}
							if (StringUtils.isNotEmpty(propertyName))
								break;
						}
						if (idProperty.isComposite())
							continue;
						if (StringUtils.isEmpty(propertyName)
								&& idProperty != null)
							propertyName = idProperty.getName();
					}

					PropertyDescriptor pd = new PropertyDescriptor(
							propertyName, obj.getClass());
					Method method = pd.getWriteMethod();
					
					if(isMap){
						if (isHQL)
							method.invoke(obj, map.get(s[1].substring(1)));
						else//无法进入的分支
							method.invoke(obj, map.get(columnName));
					} else
						method.invoke(obj, map.get(String.valueOf(index - 1 )));
				} else {
					// select 表名与where条件的表名前缀不匹配
				}
			}
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IntrospectionException e) {
			e.printStackTrace();
		}
		return obj;
	}

	public SortedMap<String, DataSource> lookupDataSourcesByRouter(
			final String methodName, final Object parameterObject) {
		SortedMap<String, DataSource> resultMap = new TreeMap<String, DataSource>();
		if (getRouter() != null && getCobarDataSourceService() != null) {
			List<String> dsSet = new ArrayList<String>();
			if (parameterObject instanceof Collection) {
				for (Iterator iterator = ((Collection) parameterObject)
						.iterator(); iterator.hasNext();) {
					List<String> tmpDS = getRouter().doRoute(
							new HibernateRoutingFact(methodName, iterator
									.next())).getResourceIdentities();
					dsSet.addAll(tmpDS);
				}
			} else {
				dsSet = getRouter().doRoute(
						new HibernateRoutingFact(methodName, parameterObject))
						.getResourceIdentities();
			}
			if (CollectionUtils.isNotEmpty(dsSet)) {
				Collections.sort(dsSet);
				for (String dsName : dsSet) {
					resultMap.put(dsName, getCobarDataSourceService()
							.getDataSources().get(dsName));
				}
			}
		}
		return resultMap;
	}

	@Override
	public void afterPropertiesSet() {
		super.afterPropertiesSet();
		if (isProfileLongTimeRunningSql()) {
			if (longTimeRunningSqlIntervalThreshold <= 0) {
				throw new IllegalArgumentException(
						"'longTimeRunningSqlIntervalThreshold' should have a positive value if 'profileLongTimeRunningSql' is set to true");
			}
		}
		setupDefaultExecutorServicesIfNecessary();
		setUpDefaultSqlAuditorExecutorIfNecessary();
		if (getConcurrentRequestProcessor() == null) {
			setConcurrentRequestProcessor(new DefaultConcurrentRequestProcessor(
					getSessionFactory()));
		}

		initTableAndClassMappers();
		initHibernateConfiguration();
	}

	private Configuration initHibernateConfiguration() {
		if (configuration == null) {
			LocalSessionFactoryBean factory = (LocalSessionFactoryBean) context
					.getBean("&sessionFactory");
			configuration = factory.getConfiguration();
		}
		return configuration;
	}

	private void initTableAndClassMappers() {
		@SuppressWarnings("unchecked")
		Map<String, AbstractEntityPersister> metaMap = getSessionFactory()
				.getAllClassMetadata();
		for (Entry<String, AbstractEntityPersister> entry : metaMap.entrySet()) {
			AbstractEntityPersister classMetadata = entry.getValue();
			String tableName = classMetadata.getTableName().toLowerCase();
			int index = tableName.indexOf(".");
			if (index >= 0) {
				tableName = tableName.substring(index + 1);
			}
			// String className = classMetadata.getEntityMetamodel().getName() ;
			String className = entry.getKey();
			//处理hibernate开发中一个table映射多个Bean的情况,数据结构由tableName->className变成tableName->className,className
			if(tableAndClassMappers.containsKey(tableName)){
				String existClassName = tableAndClassMappers.get(tableName) ;
				className = existClassName + CLASSNAME_SPLIT_STR + className ;
				logger.error("Existing tables are more than one mapping Class! The tableName is {} , the className is {} ", tableName, className);
			}
			tableAndClassMappers.put(tableName, className);
		}
	}

	/**
	 * if a SqlAuditor is injected and a sqlAuditorExecutor is NOT provided
	 * together, we need to setup a sqlAuditorExecutor so that the SQL auditing
	 * actions can be performed asynchronously. <br>
	 * otherwise, the data access process may be blocked by auditing SQL.<br>
	 * Although an external ExecutorService can be injected for use, normally,
	 * it's not so necessary.<br>
	 * Most of the time, you should inject an proper {@link ISqlAuditor} which
	 * will do SQL auditing in a asynchronous way.<br>
	 */
	private void setUpDefaultSqlAuditorExecutorIfNecessary() {
		if (sqlAuditor != null && sqlAuditorExecutor == null) {
			sqlAuditorExecutor = createCustomExecutorService(1,
					"setUpDefaultSqlAuditorExecutorIfNecessary");
			// 1. register executor for disposing later explicitly
			internalExecutorServiceRegistry.add(sqlAuditorExecutor);
			// 2. dispose executor implicitly
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					if (sqlAuditorExecutor == null) {
						return;
					}
					try {
						sqlAuditorExecutor.shutdown();
						sqlAuditorExecutor
								.awaitTermination(5, TimeUnit.MINUTES);
					} catch (InterruptedException e) {
						logger.warn(
								"interrupted when shuting down the query executor:\n{}",
								e);
					}
				}
			});
		}
	}

	/**
	 * If more than one data sources are involved in a data access request, we
	 * need a collection of executors to execute the request on these data
	 * sources in parallel.<br>
	 * But in case the users forget to inject a collection of executors for this
	 * purpose, we need to setup a default one.<br>
	 */
	private void setupDefaultExecutorServicesIfNecessary() {
		if (isPartitioningBehaviorEnabled()) {
			if (MapUtils.isEmpty(getDataSourceSpecificExecutors())) {
				Set<CobarDataSourceDescriptor> dataSourceDescriptors = getCobarDataSourceService()
						.getDataSourceDescriptors();
				for (CobarDataSourceDescriptor descriptor : dataSourceDescriptors) {
					ExecutorService executor = createExecutorForSpecificDataSource(descriptor);
					getDataSourceSpecificExecutors().put(
							descriptor.getIdentity(), executor);
				}
			}
			addDefaultSingleThreadExecutorIfNecessary();
		}
	}

	private void addDefaultSingleThreadExecutorIfNecessary() {
		String identity = getDefaultDataSourceName();
		CobarDataSourceDescriptor descriptor = new CobarDataSourceDescriptor();
		descriptor.setIdentity(identity);
		descriptor.setPoolSize(Runtime.getRuntime().availableProcessors() * 5);
		getDataSourceSpecificExecutors().put(identity,
				createExecutorForSpecificDataSource(descriptor));
	}

	private ExecutorService createExecutorForSpecificDataSource(
			CobarDataSourceDescriptor descriptor) {
		final String identity = descriptor.getIdentity();
		final ExecutorService executor = createCustomExecutorService(
				descriptor.getPoolSize(),
				"createExecutorForSpecificDataSource-" + identity
						+ " data source");
		// 1. register executor for disposing explicitly
		internalExecutorServiceRegistry.add(executor);
		// 2. dispose executor implicitly
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				if (executor == null) {
					return;
				}
				try {
					executor.shutdown();
					executor.awaitTermination(5, TimeUnit.MINUTES);
				} catch (InterruptedException e) {
					logger.warn(
							"interrupted when shuting down the query executor:\n{}",
							e);
				}
			}
		});
		return executor;
	}

	private ExecutorService createCustomExecutorService(int poolSize,
			final String method) {
		int coreSize = Runtime.getRuntime().availableProcessors();
		if (poolSize < coreSize) {
			coreSize = poolSize;
		}
		ThreadFactory tf = new ThreadFactory() {
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r,
						"thread created at CobarHibernateTemplate method ["
								+ method + "]");
				t.setDaemon(true);
				return t;
			}
		};
		BlockingQueue<Runnable> queueToUse = new LinkedBlockingQueue<Runnable>(
				coreSize);
		final ThreadPoolExecutor executor = new ThreadPoolExecutor(coreSize,
				poolSize, 60, TimeUnit.SECONDS, queueToUse, tf,
				new ThreadPoolExecutor.CallerRunsPolicy());
		return executor;
	}

	public void destroy() throws Exception {
		if (CollectionUtils.isNotEmpty(internalExecutorServiceRegistry)) {
			logger.info("shutdown executors of CobarHibernateTemplate...");
			for (ExecutorService executor : internalExecutorServiceRegistry) {
				if (executor != null) {
					try {
						executor.shutdown();
						executor.awaitTermination(5, TimeUnit.MINUTES);
						executor = null;
					} catch (InterruptedException e) {
						logger.warn(
								"interrupted when shuting down the query executor:\n{}",
								e);
					}
				}
			}
			getDataSourceSpecificExecutors().clear();
			logger.info("all of the executor services in CobarHibernateTemplate are disposed.");
		}
	}

	public void setMergers(Map<String, IMerger<Object, Object>> mergers) {
		this.mergers = mergers;
	}

	public Map<String, IMerger<Object, Object>> getMergers() {
		return mergers;
	}

	public void setSqlAuditor(ISqlAuditor sqlAuditor) {
		this.sqlAuditor = sqlAuditor;
	}

	public ISqlAuditor getSqlAuditor() {
		return sqlAuditor;
	}

	public void setSqlAuditorExecutor(ExecutorService sqlAuditorExecutor) {
		this.sqlAuditorExecutor = sqlAuditorExecutor;
	}

	public ExecutorService getSqlAuditorExecutor() {
		return sqlAuditorExecutor;
	}

	public void setCobarDataSourceService(
			ICobarDataSourceService cobarDataSourceService) {
		this.cobarDataSourceService = cobarDataSourceService;
	}

	public ICobarDataSourceService getCobarDataSourceService() {
		return cobarDataSourceService;
	}

	public void setProfileLongTimeRunningSql(boolean profileLongTimeRunningSql) {
		this.profileLongTimeRunningSql = profileLongTimeRunningSql;
	}

	public boolean isProfileLongTimeRunningSql() {
		return profileLongTimeRunningSql;
	}

	public void setLongTimeRunningSqlIntervalThreshold(
			long longTimeRunningSqlIntervalThreshold) {
		this.longTimeRunningSqlIntervalThreshold = longTimeRunningSqlIntervalThreshold;
	}

	public long getLongTimeRunningSqlIntervalThreshold() {
		return longTimeRunningSqlIntervalThreshold;
	}

	public void setDefaultDataSourceName(String defaultDataSourceName) {
		this.defaultDataSourceName = defaultDataSourceName;
	}

	public String getDefaultDataSourceName() {
		return defaultDataSourceName;
	}

	/**
	 * if a router and a data source locator is provided, it means data access
	 * on different databases is enabled.<br>
	 */
	protected boolean isPartitioningBehaviorEnabled() {
		return true;
	}

	public void setDataSourceSpecificExecutors(
			Map<String, ExecutorService> dataSourceSpecificExecutors) {
		if (MapUtils.isEmpty(dataSourceSpecificExecutors)) {
			return;
		}
		this.dataSourceSpecificExecutors = dataSourceSpecificExecutors;
	}

	public Map<String, ExecutorService> getDataSourceSpecificExecutors() {
		return dataSourceSpecificExecutors;
	}

	public void setRouter(ICobarRouter<HibernateRoutingFact> router) {
		this.router = router;
	}

	public ICobarRouter<HibernateRoutingFact> getRouter() {
		return router;
	}

	public void setConcurrentRequestProcessor(
			IConcurrentRequestProcessor concurrentRequestProcessor) {
		this.concurrentRequestProcessor = concurrentRequestProcessor;
	}

	public IConcurrentRequestProcessor getConcurrentRequestProcessor() {
		return concurrentRequestProcessor;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.context = applicationContext;
	}

	public static void main(String[] args) {
		/*
		CobarHibernateTemplate cht = new CobarHibernateTemplate();
		List<String> list = cht
				.getParameters(
						"delete  FROM MidPlyRiskUnitVO WHERE CPlyAppNo = :CPlyAppNo and CIsRead =1",
						null);
		System.out.println(list);
		
		List<String> list1 = cht
				.getParameters(
						"select *  FROM MidPlyRiskUnitVO WHERE CPlyAppNo =? and CIsRead =1",
						null);
		System.out.println(list1);
		
		String condition = list.get(0);
		String[] s = condition.split("=");
		for (int ca = 0; ca < s.length; ca++) {
			System.out.println("ddddd" + s[ca] + "aaaaaaaa");
			s[ca] = s[ca].trim();
		}

		for (int ca = 0; ca < s.length; ca++) {
			System.out.println("ddddd" + s[ca] + "aaaaaaaa");
		}
		*/
	}
}
