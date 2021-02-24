package com.alibaba.cobar.client.router.rules;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.List;

import org.testng.annotations.Test;

import com.alibaba.cobar.client.router.rules.hibernate.HibernateClassNameRule;
import com.alibaba.cobar.client.router.support.HibernateRoutingFact;
import com.alibaba.cobar.client.support.utils.CollectionUtils;

@Test
public class HibernateClassNameRuleTest{
    public void testNamespaceRuleNormally() {
        HibernateClassNameRule rule = new HibernateClassNameRule("com.alibaba.cobar.client.entities.User","p1, p2");
        List<String> shardIds = rule.action();
        assertNotNull(shardIds);
        assertEquals(2, shardIds.size());

        HibernateRoutingFact fact = new HibernateRoutingFact("com.alibaba.cobar.client.entities.User.save", null);
        assertTrue(rule.isDefinedAt(fact));
        fact = new HibernateRoutingFact("com.alibaba.cobar.client.entities.User.delete", null);
        assertTrue(rule.isDefinedAt(fact));
        fact = new HibernateRoutingFact("com.alibaba.cobar.client.entities.User.delete", null);
        assertTrue(rule.isDefinedAt(fact));
    }

    public void testNamespaceRuleNormallyWithCustomActionPatternSeparator() {
    	HibernateClassNameRule rule = new HibernateClassNameRule("com.alibaba.cobar.client.entities.User",
                "p1, p2");
        rule.setActionPatternSeparator(";");
        List<String> shards = rule.action();
        assertTrue(CollectionUtils.isNotEmpty(shards));
        assertEquals(1, shards.size());

        rule = new HibernateClassNameRule("com.alibaba.cobar.client.entities.Tweet", "p1; p2");
        rule.setActionPatternSeparator(";");
        shards = null;
        shards = rule.action();
        assertTrue(CollectionUtils.isNotEmpty(shards));
        assertEquals(2, shards.size());

        HibernateRoutingFact fact = new HibernateRoutingFact(
                "com.alibaba.cobar.client.entities.Tweet.update", null);
        assertTrue(rule.isDefinedAt(fact));
        fact = new HibernateRoutingFact("com.alibaba.cobar.client.entities.Tweet.delete", null);
        assertTrue(rule.isDefinedAt(fact));
        fact = new HibernateRoutingFact("com.alibaba.cobar.client.entities.Twet.delete", null);
        assertFalse(rule.isDefinedAt(fact));
    }

    public void testNamespaceRuleAbnormally() {
        try {
            new HibernateClassNameRule("", "");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }

        try {
            new HibernateClassNameRule("", null);
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }

        try {
            new HibernateClassNameRule(null, "");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }

        try {
            new HibernateClassNameRule(null, null);
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }

        HibernateClassNameRule rule = new HibernateClassNameRule("com.alibaba.cobar.client.entities.Tweet",
                "p1, p2");
        try {
            rule.setActionPatternSeparator(null);
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
}
