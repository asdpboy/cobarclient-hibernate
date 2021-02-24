package com.alibaba.cobar.client.router.rules.hibernate;

import com.alibaba.cobar.client.router.support.HibernateRoutingFact;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;

/**
 * @author yanhongqi
 * @since 1.0
 */
public class HibernateMethodNameRule extends AbstractHibernateOrientedRule {

	public HibernateMethodNameRule(String pattern, String action) {
		super(pattern, action);
		// TODO Auto-generated constructor stub
	}

	public boolean isDefinedAt(HibernateRoutingFact routingFact) {
		Validate.notNull(routingFact);
        return StringUtils.equals(getTypePattern(), routingFact.getMethod());
	}

    @Override
    public String toString() {
        return "HibernateMethodNameRule [getAction()=" + getAction() + ", getTypePattern()="
                + getTypePattern() + "]";
    }

}
