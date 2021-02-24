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
import org.apache.commons.lang.StringUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.alibaba.cobar.client.router.rules.hibernate.HibernateClassNameShardingRule;
import com.alibaba.cobar.client.router.rules.support.ModFunction;
import com.alibaba.cobar.client.router.support.HibernateRoutingFact;
import com.alibaba.cobar.client.support.utils.CollectionUtils;
@Test
public class HibernateClassNameShardingRuleTest {

    public static final String          DEFAULT_TYPE_PATTEN      = "com.alibaba.cobar.client.entities.User";
    public static final String          DEFAULT_SHARDING_PATTERN = "id>=10000 and id < 20000";
    public static final String[]        DEFAULT_SHARDS           = { "shard1", "shard2" };

    private HibernateClassNameShardingRule rule;

    @BeforeMethod
    protected void setUp() throws Exception {
        rule = new HibernateClassNameShardingRule(DEFAULT_TYPE_PATTEN, "shard1,shard2",DEFAULT_SHARDING_PATTERN);
    }
    @AfterMethod
    protected void tearDown() throws Exception {
        rule = null;
    }

    public void testShardIdAssemblyNormally() {
        List<String> shards = rule.action();
        assertTrue(CollectionUtils.isNotEmpty(shards));
        assertEquals(2, shards.size());

        for (String shard : shards) {
            assertTrue(ArrayUtils.contains(DEFAULT_SHARDS, shard));
        }
    }

    public void testShardIdAssemblyAbnormally() {
        try {
            new HibernateClassNameShardingRule(DEFAULT_TYPE_PATTEN, null, DEFAULT_SHARDING_PATTERN);
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }

        try {
            new HibernateClassNameShardingRule(DEFAULT_TYPE_PATTEN, "", DEFAULT_SHARDING_PATTERN);
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
    }

    public void testShardIdAssemblyWithCustomActionPatternSeparatorNormally() {
        rule.setActionPatternSeparator(";");
        List<String> shards = rule.action();
        assertTrue(CollectionUtils.isNotEmpty(shards));
        assertEquals(1, shards.size());
        assertEquals("shard1,shard2", shards.get(0));

        rule = new HibernateClassNameShardingRule(DEFAULT_TYPE_PATTEN, "shard1;shard2",
                DEFAULT_SHARDING_PATTERN);
        rule.setActionPatternSeparator(";");
        shards = null;
        shards = rule.action();
        assertTrue(CollectionUtils.isNotEmpty(shards));
        assertEquals(2, shards.size());
        for (String shard : shards) {
            assertTrue(ArrayUtils.contains(DEFAULT_SHARDS, shard));
        }
    }

    public void testShardIdAssemblyWithCustomActionPatternSeparatorAbnormally() {
        try {
            rule.setActionPatternSeparator(null);
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
    }

    public void testRuleTypePatternMatchingNormally() {
        User t = new User();
        t.setId(15000);
        t.setUsername("anything");
        HibernateRoutingFact fact = new HibernateRoutingFact(
                "com.alibaba.cobar.client.entities.User.create", t);
        assertTrue(rule.isDefinedAt(fact));

        fact = new HibernateRoutingFact("com.alibaba.cobar.client.entities.User.update", t);
        assertTrue(rule.isDefinedAt(fact));

        fact = new HibernateRoutingFact("com.alibaba.cobar.client.entities.Follower.update", t);
        assertFalse(rule.isDefinedAt(fact));
    }

    public void testRuleTypePatternMatchingAbnormally() {
        // abnormal parameter 
        try {
            rule.isDefinedAt(null);
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }

        // construction abnormally
        try {
            rule = new HibernateClassNameShardingRule(null, "shard1,shard2", DEFAULT_SHARDING_PATTERN);
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
        try {
            rule = new HibernateClassNameShardingRule("", "shard1,shard2", DEFAULT_SHARDING_PATTERN);
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
    }

    public void testRuleShardingPatternMatchingNormally() {
        User t = new User();
        t.setId(15000);
        t.setUsername("anything");
        HibernateRoutingFact fact = new HibernateRoutingFact(
                "com.alibaba.cobar.client.entities.User.update", t);
        assertTrue(rule.isDefinedAt(fact));

        t.setId(20000001);
        fact = new HibernateRoutingFact("com.alibaba.cobar.client.entities.User.update", t);
        assertFalse(rule.isDefinedAt(fact));

        fact = new HibernateRoutingFact("com.alibaba.cobar.client.entities.User.update", null);
        assertFalse(rule.isDefinedAt(fact));

        Map<String, Long> ctx = new HashMap<String, Long>();
        fact = new HibernateRoutingFact("com.alibaba.cobar.client.entities.User.update", ctx);
        assertFalse(rule.isDefinedAt(fact));

        ctx.put("id", 18000L);
        assertTrue(rule.isDefinedAt(fact));

    }

    public void testRuleShardingPatternMatchingAbnormally() {
        try {
            new HibernateClassNameShardingRule(DEFAULT_TYPE_PATTEN, "shard1,shard2", null);
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }

        try {
            new HibernateClassNameShardingRule(DEFAULT_TYPE_PATTEN, "shard1,shard2", "");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
    }

    public void testRuleShardingPatternWithCustomFunctions() throws Exception {
        String shardingExpression = "mod.apply(id)==3";
        HibernateClassNameShardingRule r = new HibernateClassNameShardingRule(DEFAULT_TYPE_PATTEN,
                StringUtils.join(DEFAULT_SHARDS, ","), shardingExpression);
        Map<String, Object> functions = new HashMap<String, Object>();
        functions.put("mod", new ModFunction(18L));
        r.setFunctionMap(functions);

        User t = new User();
        t.setId(3);
        t.setUsername("anything");
        HibernateRoutingFact fact = new HibernateRoutingFact(
                "com.alibaba.cobar.client.entities.User.create", t);
        assertTrue(r.isDefinedAt(fact));
    }
    
    public void testRuleExpressionEvaluationWithSimpleTypeRoutingFact()
    {
        HibernateClassNameShardingRule r = new HibernateClassNameShardingRule(DEFAULT_TYPE_PATTEN,
                "shard2", "$ROOT.startsWith(\"A\")");
        
        HibernateRoutingFact fact = new HibernateRoutingFact(
                "com.alibaba.cobar.client.entities.User.create", "Arron");
        assertTrue(r.isDefinedAt(fact));
        
        r = new HibernateClassNameShardingRule(DEFAULT_TYPE_PATTEN,
                "shard2", "startsWith(\"A\")");
        assertTrue(r.isDefinedAt(fact));
        
        fact = new HibernateRoutingFact(
                "com.alibaba.cobar.client.entities.User.create", "Donald");
        assertFalse(r.isDefinedAt(fact));
    }
}
