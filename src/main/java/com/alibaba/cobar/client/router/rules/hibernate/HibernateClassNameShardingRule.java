package com.alibaba.cobar.client.router.rules.hibernate;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.cobar.client.router.support.HibernateRoutingFact;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.mvel2.MVEL;
import org.mvel2.integration.VariableResolverFactory;
import org.mvel2.integration.impl.MapVariableResolverFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yanhongqi
 * @since 1.0
 */
public class HibernateClassNameShardingRule extends
		AbstractHibernateOrientedRule {

	 private transient final Logger logger = LoggerFactory
             .getLogger(HibernateClassNameShardingRule.class);

	
	public HibernateClassNameShardingRule(String pattern, String action,
			String attributePattern) {
		super(pattern, action, attributePattern);
	}

	public boolean isDefinedAt(HibernateRoutingFact routingFact) {
		Validate.notNull(routingFact);
        String className = StringUtils.substringBeforeLast(routingFact.getMethod(), ".");
        boolean matches = StringUtils.equals(className, getTypePattern());
        if (matches) {
            try {
                Map<String, Object> vrs = new HashMap<String, Object>();
                vrs.putAll(getFunctionMap());
                vrs.put("$ROOT", routingFact.getArgument()); // add top object reference for expression
                VariableResolverFactory vrfactory = new MapVariableResolverFactory(vrs);
                if (MVEL.evalToBoolean(getAttributePattern(), routingFact.getArgument(), vrfactory)) {
                    return true;
                }
            } catch (Throwable t) {
                logger
                        .info(
                                "failed to evaluate attribute expression:'{}' with context object:'{}'\n{}",
                                new Object[] { getAttributePattern(), routingFact.getArgument(), t });
            }
        }
        return false;
	}

	@Override
	public String toString() {
		return super.toString();
	}
}
