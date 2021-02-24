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
 package com.alibaba.cobar.client.router.config.support;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.cobar.client.router.support.HibernateRoutingFact;
import com.alibaba.cobar.client.support.utils.CollectionUtils;
import com.alibaba.cobar.client.support.utils.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;

import com.alibaba.cobar.client.router.DefaultCobarClientInternalRouter;
import com.alibaba.cobar.client.router.config.vo.InternalRule;
import com.alibaba.cobar.client.router.rules.IRoutingRule;
import com.alibaba.cobar.client.router.rules.hibernate.HibernateClassNameRule;
import com.alibaba.cobar.client.router.rules.hibernate.HibernateClassNameShardingRule;
import com.alibaba.cobar.client.router.rules.hibernate.HibernateMethodNameRule;
import com.alibaba.cobar.client.router.rules.hibernate.HibernateMethodNameShardingRule;

public class InternalRuleLoader4DefaultInternalRouter {

    public void loadRulesAndEquipRouter(List<InternalRule> rules,
                                        DefaultCobarClientInternalRouter router,
                                        Map<String, Object> functionsMap) {
        if (CollectionUtils.isEmpty(rules)) {
            return;
        }

        for (InternalRule rule : rules) {
            String className = StringUtils.trimToEmpty(rule.getClassName());
            String methodName = StringUtils.trimToEmpty(rule.getMethodName());
            String shardingExpression = StringUtils.trimToEmpty(rule.getShardingExpression());
            String destinations = StringUtils.trimToEmpty(rule.getShards());

            Validate.notEmpty(destinations, "destination shards must be given explicitly.");

            if (StringUtils.isEmpty(className) && StringUtils.isEmpty(methodName)) {
                throw new IllegalArgumentException(
                        "at least one of 'class' or 'method' must be given.");
            }
            if (StringUtils.isNotEmpty(className) && StringUtils.isNotEmpty(methodName)) {
                throw new IllegalArgumentException(
                        "'class' and 'method' are alternatives, can't guess which one to use if both of them are provided.");
            }

            if (StringUtils.isNotEmpty(className)) {
                List<Set<IRoutingRule<HibernateRoutingFact, List<String>>>> ruleSequence = setUpRuleSequenceContainerIfNecessary(
                        router, className);

                if (StringUtils.isEmpty(shardingExpression)) {

                    ruleSequence.get(3).add(new HibernateClassNameRule(className, destinations));
                } else {
                	HibernateClassNameShardingRule insr = new HibernateClassNameShardingRule(className,
                            destinations, shardingExpression);
                    if (MapUtils.isNotEmpty(functionsMap)) {
                        insr.setFunctionMap(functionsMap);
                    }
                    ruleSequence.get(2).add(insr);
                }
            }
            if (StringUtils.isNotEmpty(methodName)) {
                List<Set<IRoutingRule<HibernateRoutingFact, List<String>>>> ruleSequence = setUpRuleSequenceContainerIfNecessary(
                        router, StringUtils.substringBeforeLast(methodName, "."));

                if (StringUtils.isEmpty(shardingExpression)) {
                    ruleSequence.get(1).add(new HibernateMethodNameRule(methodName, destinations));
                } else {
                	HibernateMethodNameShardingRule issr = new HibernateMethodNameShardingRule(methodName,
                            destinations, shardingExpression);
                    if (MapUtils.isNotEmpty(functionsMap)) {
                        issr.setFunctionMap(functionsMap);
                    }
                    ruleSequence.get(0).add(issr);
                }
            }
        }
    }

    private List<Set<IRoutingRule<HibernateRoutingFact, List<String>>>> setUpRuleSequenceContainerIfNecessary(
                                                                                                           DefaultCobarClientInternalRouter routerToUse,
                                                                                                           String className) {
        List<Set<IRoutingRule<HibernateRoutingFact, List<String>>>> ruleSequence = routerToUse
                .getRulesGroupByClassNames().get(className);
        if (CollectionUtils.isEmpty(ruleSequence)) {
            ruleSequence = new ArrayList<Set<IRoutingRule<HibernateRoutingFact, List<String>>>>();
            Set<IRoutingRule<HibernateRoutingFact, List<String>>> methodShardingRules = new HashSet<IRoutingRule<HibernateRoutingFact, List<String>>>();
            Set<IRoutingRule<HibernateRoutingFact, List<String>>> methodRules = new HashSet<IRoutingRule<HibernateRoutingFact, List<String>>>();
            Set<IRoutingRule<HibernateRoutingFact, List<String>>> classShardingRules = new HashSet<IRoutingRule<HibernateRoutingFact, List<String>>>();
            Set<IRoutingRule<HibernateRoutingFact, List<String>>> classRules = new HashSet<IRoutingRule<HibernateRoutingFact, List<String>>>();
            ruleSequence.add(methodShardingRules);
            ruleSequence.add(methodRules);
            ruleSequence.add(classShardingRules);
            ruleSequence.add(classRules);
            routerToUse.getRulesGroupByClassNames().put(className, ruleSequence);
        }
        return ruleSequence;
    }
}
