package com.alibaba.cobar.client.router.rules.hibernate;

import com.alibaba.cobar.client.router.support.HibernateRoutingFact;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;

/**
 * @author yanhongqi
 * @since 1.0
 */
public class HibernateClassNameRule extends AbstractHibernateOrientedRule {

	public HibernateClassNameRule(String typePattern, String action) {
		super(typePattern, action);
	}

	public boolean isDefinedAt(HibernateRoutingFact routingFact) {
		Validate.notNull(routingFact);
		String className = StringUtils.substringBeforeLast(routingFact.getMethod(), ".");
        return StringUtils.equals(className, getTypePattern());
	}
	

    @Override
    public String toString() {
        return "HibernateClassNameRule [getAction()=" + getAction() + ", getTypePattern()="
                + getTypePattern() + "]";
    }
}
