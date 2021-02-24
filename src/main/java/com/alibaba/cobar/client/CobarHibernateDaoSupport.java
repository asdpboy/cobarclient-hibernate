package com.alibaba.cobar.client;

import org.springframework.orm.hibernate3.support.HibernateDaoSupport;

/**
 * @author yanhongqi
 * @since 1.0
 */
public class CobarHibernateDaoSupport extends HibernateDaoSupport {
	protected boolean isPartitionBehaviorEnabled() {
        if (getHibernateTemplate() instanceof CobarHibernateTemplate) {
            return ((CobarHibernateTemplate) getHibernateTemplate())
                    .isPartitioningBehaviorEnabled();
        }
        return false;
    }
}
