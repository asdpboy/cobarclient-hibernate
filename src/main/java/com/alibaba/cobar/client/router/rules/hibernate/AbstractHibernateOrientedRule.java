package com.alibaba.cobar.client.router.rules.hibernate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.cobar.client.router.support.HibernateRoutingFact;
import com.alibaba.cobar.client.support.utils.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;

import com.alibaba.cobar.client.router.rules.AbstractEntityAttributeRule;

/**
 * @author yanhongqi
 * @since 1.0
 */
public abstract class AbstractHibernateOrientedRule extends AbstractEntityAttributeRule<HibernateRoutingFact, List<String>> {
	
	public static final String DEFAULT_DATASOURCE_IDENTITY_SEPARATOR = ",";
	
	private Map<String, Object> functionMap = new HashMap<String, Object>();
	
	private String actionPatternSeparator = DEFAULT_DATASOURCE_IDENTITY_SEPARATOR;
	
	private List<String> dataSourceIds = new ArrayList<String>();

	public AbstractHibernateOrientedRule(String typePattern, String action) {
		super(typePattern, action);
	}
	

    public AbstractHibernateOrientedRule(String pattern, String action, String attributePattern) {
        super(pattern, action, attributePattern);
    }

    public synchronized List<String> action() {
        if(CollectionUtils.isEmpty(dataSourceIds))
        {
            List<String> ids = new ArrayList<String>();
            for (String id : StringUtils.split(getAction(), getActionPatternSeparator())) {
                ids.add(StringUtils.trimToEmpty(id));
            }
            setDataSourceIds(ids);
        }
        return dataSourceIds;
    }

    public void setDataSourceIds(List<String> dataSourceIds) {
        this.dataSourceIds = dataSourceIds;
    }

    public List<String> getDataSourceIds() {
        return dataSourceIds;
    }

    public void setActionPatternSeparator(String actionPatternSeparator) {
        Validate.notNull(actionPatternSeparator);
        this.actionPatternSeparator = actionPatternSeparator;
    }

    public String getActionPatternSeparator() {
        return actionPatternSeparator;
    }

    public void setFunctionMap(Map<String, Object> functionMap) {
        this.functionMap = functionMap;
    }

    public Map<String, Object> getFunctionMap() {
        return functionMap;
    }
}
