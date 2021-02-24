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
 package com.alibaba.cobar.client.router.config;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.alibaba.cobar.client.router.config.vo.InternalRule;
import com.alibaba.cobar.client.router.config.vo.InternalRules;
import com.alibaba.cobar.client.router.rules.hibernate.HibernateClassNameRule;
import com.alibaba.cobar.client.router.rules.hibernate.HibernateClassNameShardingRule;
import com.alibaba.cobar.client.router.support.HibernateRoutingFact;
import com.alibaba.cobar.client.support.utils.CollectionUtils;
import com.alibaba.cobar.client.support.utils.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.springframework.core.io.Resource;

import com.alibaba.cobar.client.router.CobarClientInternalRouter;
import com.alibaba.cobar.client.router.rules.IRoutingRule;
import com.thoughtworks.xstream.XStream;

/**
 * @author yanhongqi
 * @since 1.0
 */
public class CobarInteralRouterXmlFactoryBean extends
        AbstractCobarInternalRouterConfigurationFactoryBean {

    @Override
    protected void assembleRulesForRouter(
                                          CobarClientInternalRouter router,
                                          Resource configLocation,
                                          Set<IRoutingRule<HibernateRoutingFact, List<String>>> sqlActionShardingRules,
                                          Set<IRoutingRule<HibernateRoutingFact, List<String>>> sqlActionRules,
                                          Set<IRoutingRule<HibernateRoutingFact, List<String>>> namespaceShardingRules,
                                          Set<IRoutingRule<HibernateRoutingFact, List<String>>> namespaceRules)
            throws IOException {
        XStream xstream = new XStream();
        xstream.alias("rules", InternalRules.class);
        xstream.alias("rule", InternalRule.class);
        xstream.addImplicitCollection(InternalRules.class, "rules");
        xstream.useAttributeFor(InternalRule.class, "merger");

        InternalRules internalRules = (InternalRules) xstream.fromXML(configLocation
                .getInputStream());
        List<InternalRule> rules = internalRules.getRules();
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
                        "at least one of 'namespace' or 'sqlAction' must be given.");
            }
            if (StringUtils.isNotEmpty(className) && StringUtils.isNotEmpty(methodName)) {
                throw new IllegalArgumentException(
                        "'namespace' and 'sqlAction' are alternatives, can't guess which one to use if both of them are provided.");
            }

            if (StringUtils.isNotEmpty(className)) {
                if (StringUtils.isEmpty(shardingExpression)) {
                    namespaceRules.add(new HibernateClassNameRule(className, destinations));
                } else {
                	HibernateClassNameShardingRule insr = new HibernateClassNameShardingRule(className,
                            destinations, shardingExpression);
                    if (MapUtils.isNotEmpty(getFunctionsMap())) {
                        insr.setFunctionMap(getFunctionsMap());
                    }
                    namespaceShardingRules.add(insr);
                }
            }
            if (StringUtils.isNotEmpty(methodName)) {
                if (StringUtils.isEmpty(shardingExpression)) {
                    sqlActionRules.add(new HibernateClassNameRule(methodName, destinations));
                } else {
                	HibernateClassNameShardingRule issr = new HibernateClassNameShardingRule(methodName,
                            destinations, shardingExpression);
                    if (MapUtils.isNotEmpty(getFunctionsMap())) {
                        issr.setFunctionMap(getFunctionsMap());
                    }
                    sqlActionShardingRules.add(issr);
                }
            }
        }

    }

}
