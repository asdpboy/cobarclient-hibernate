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
 package com.alibaba.cobar.client.router;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.cobar.client.router.config.DefaultCobarClientInternalRouterXmlFactoryBean;
import com.alibaba.cobar.client.router.rules.IRoutingRule;
import com.alibaba.cobar.client.router.support.HibernateRoutingFact;
import com.alibaba.cobar.client.router.support.RoutingResult;
import com.alibaba.cobar.client.support.utils.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link DefaultCobarClientInternalRouter} receive a map which will maintain a
 * group of rules as per SQ-map namespaces.<br>
 * it will evaluate the rules as per namesapce and a sequence from specific
 * rules to more generic rules.<br>
 * usually, the users don't need to care about these internal details, to use
 * {@link DefaultCobarClientInternalRouter}, just turn to
 * {@link DefaultCobarClientInternalRouterXmlFactoryBean} for instantiation.<br>
 * 
 * @author fujohnwang
 * @since 1.0
 * @see DefaultCobarClientInternalRouterXmlFactoryBean
 */
public class DefaultCobarClientInternalRouter implements ICobarRouter<HibernateRoutingFact> {

    private transient final Logger                                                logger                 = LoggerFactory
                                                                                                                 .getLogger(DefaultCobarClientInternalRouter.class);

    private Map<String, List<Set<IRoutingRule<HibernateRoutingFact, List<String>>>>> rulesGroupByClassNames = new HashMap<String, List<Set<IRoutingRule<HibernateRoutingFact, List<String>>>>>();

    public RoutingResult doRoute(HibernateRoutingFact routingFact) throws RoutingException {
        Validate.notNull(routingFact);
        String methodName = routingFact.getMethod() ;
        Validate.notEmpty(methodName);
        String className = StringUtils.substringBeforeLast(methodName, ".");
        Validate.notEmpty(className);
        
        List<Set<IRoutingRule<HibernateRoutingFact, List<String>>>> rules = getRulesGroupByClassNames()
                .get(className);

        RoutingResult result = new RoutingResult();
        result.setResourceIdentities(new ArrayList<String>());

        if (!CollectionUtils.isEmpty(rules)) {
            IRoutingRule<HibernateRoutingFact, List<String>> ruleToUse = null;
            for (Set<IRoutingRule<HibernateRoutingFact, List<String>>> ruleSet : rules) {
                ruleToUse = searchMatchedRuleAgainst(ruleSet, routingFact);
                if (ruleToUse != null) {
                    break;
                }
            }

            if (ruleToUse != null) {
                logger.info("matched with rule:{} with fact:{}", ruleToUse, routingFact);
                result.getResourceIdentities().addAll(ruleToUse.action());
            } else {
                logger.info("No matched rule found for routing fact:{}", routingFact);
            }
        }

        return result;
    }

    private IRoutingRule<HibernateRoutingFact, List<String>> searchMatchedRuleAgainst(
                                                                                   Set<IRoutingRule<HibernateRoutingFact, List<String>>> rules,
                                                                                   HibernateRoutingFact routingFact) {
        if (CollectionUtils.isEmpty(rules)) {
            return null;
        }
        for (IRoutingRule<HibernateRoutingFact, List<String>> rule : rules) {
            if (rule.isDefinedAt(routingFact)) {
                return rule;
            }
        }
        return null;
    }

	public Map<String, List<Set<IRoutingRule<HibernateRoutingFact, List<String>>>>> getRulesGroupByClassNames() {
		return rulesGroupByClassNames;
	}

	public void setRulesGroupByClassNames(
			Map<String, List<Set<IRoutingRule<HibernateRoutingFact, List<String>>>>> rulesGroupByClassNames) {
		this.rulesGroupByClassNames = rulesGroupByClassNames;
	}

}
