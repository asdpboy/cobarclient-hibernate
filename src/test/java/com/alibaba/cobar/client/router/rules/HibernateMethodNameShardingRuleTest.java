package com.alibaba.cobar.client.router.rules;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.cobar.client.entities.User;
import org.apache.commons.lang.ArrayUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.alibaba.cobar.client.router.rules.hibernate.HibernateMethodNameShardingRule;
import com.alibaba.cobar.client.router.rules.support.ModFunction;
import com.alibaba.cobar.client.router.support.HibernateRoutingFact;
import com.alibaba.cobar.client.support.utils.CollectionUtils;

/**
 * TODO Comment of HibernateMethodNameShardingRuleTest
 * 
 * @author fujohnwang
 * @see {@link HibernateClassNameShardingRuleTest} for more test scenarios.
 */
@Test
public class HibernateMethodNameShardingRuleTest{
    // almost copied from IBatisNamespaceShardingRuleTest, although a same top class is better.
    public static final String          DEFAULT_TYPE_PATTEN      = "com.alibaba.cobar.client.entities.User.save";
    public static final String          DEFAULT_SHARDING_PATTERN = "id>=10000 and id < 20000";
    public static final String[]        DEFAULT_SHARDS           = { "shard1", "shard2" };

    private HibernateMethodNameShardingRule rule;

    @BeforeMethod
    protected void setUp() throws Exception {
        rule = new HibernateMethodNameShardingRule(DEFAULT_TYPE_PATTEN, "shard1,shard2",
                DEFAULT_SHARDING_PATTERN);
    }
    @AfterMethod
    protected void tearDown() throws Exception {
        rule = null;
    }

    public void testSqlActionShardingRuleConstructionAbnormally() {
        try {
            rule = new HibernateMethodNameShardingRule(null, "shard1,shard2", DEFAULT_SHARDING_PATTERN);
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }

        try {
            rule = new HibernateMethodNameShardingRule("", "shard1,shard2", DEFAULT_SHARDING_PATTERN);
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }

        try {
            rule = new HibernateMethodNameShardingRule(DEFAULT_TYPE_PATTEN, "",
                    DEFAULT_SHARDING_PATTERN);
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }

        try {
            rule = new HibernateMethodNameShardingRule(DEFAULT_TYPE_PATTEN, null,
                    DEFAULT_SHARDING_PATTERN);
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }

        try {
            rule = new HibernateMethodNameShardingRule(DEFAULT_TYPE_PATTEN, "shard1,shard2", null);
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }

        try {
            rule = new HibernateMethodNameShardingRule(DEFAULT_TYPE_PATTEN, "shard1,shard2", "");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
    }

    public void testSqlActionShardingRulePatternMatchingNormally() {
        User t = new User();
        t.setId(15000);
        t.setUsername("anything");

        HibernateRoutingFact fact = new HibernateRoutingFact(
                "com.alibaba.cobar.client.entities.User.save", t);
        assertTrue(rule.isDefinedAt(fact));
        List<String> shards = rule.action();
        assertTrue(CollectionUtils.isNotEmpty(shards));
        assertEquals(2, shards.size());
        for (String shard : shards) {
            assertTrue(ArrayUtils.contains(DEFAULT_SHARDS, shard));
        }

        fact = new HibernateRoutingFact("com.alibaba.cobar.client.entities.User.update", t);
        assertFalse(rule.isDefinedAt(fact));

        t.setId(20000);
        fact = new HibernateRoutingFact("com.alibaba.cobar.client.entities.User.save", t);
        assertFalse(rule.isDefinedAt(fact));

        fact = new HibernateRoutingFact("com.alibaba.cobar.client.entities.User.save", null);
        assertFalse(rule.isDefinedAt(fact));

        fact = new HibernateRoutingFact("com.alibaba.cobar.client.entities.User.save", new Object());
        assertFalse(rule.isDefinedAt(fact));
    }

    public void testSqlActionShardingRulePatternMatchingAbnormally() {
        try {
            rule.setActionPatternSeparator(null);
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }

        try {
            rule.isDefinedAt(null);
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
    }

    public void testSqlActionShardingRuleWithCustomFunctions() {
        HibernateMethodNameShardingRule r = new HibernateMethodNameShardingRule(DEFAULT_TYPE_PATTEN,
                "shard1,shard2", "mod.apply(id)==3");
        Map<String, Object> functions = new HashMap<String, Object>();
        functions.put("mod", new ModFunction(18L));
        r.setFunctionMap(functions);
        
        User t = new User();
        t.setId(21);
        t.setUsername("anything");
        HibernateRoutingFact fact = new HibernateRoutingFact(
                "com.alibaba.cobar.client.entities.User.save", t);
        assertTrue(r.isDefinedAt(fact));
    }
    
    public void testSqlActionShardingRuleWithSimpleContextObjectType(){
        HibernateMethodNameShardingRule r = new HibernateMethodNameShardingRule(DEFAULT_TYPE_PATTEN,
                "shard1", "$ROOT.startsWith(\"J\")");
        HibernateRoutingFact fact = new HibernateRoutingFact(
                "com.alibaba.cobar.client.entities.User.save", "Jack");
        assertTrue(r.isDefinedAt(fact));
        
        r = new HibernateMethodNameShardingRule(DEFAULT_TYPE_PATTEN,
                "shard1", "startsWith(\"J\")");
        assertTrue(r.isDefinedAt(fact));
        
        fact = new HibernateRoutingFact(
                "com.alibaba.cobar.client.entities.User.save", "Amanda");
        assertFalse(r.isDefinedAt(fact));
        
        
    }
}
