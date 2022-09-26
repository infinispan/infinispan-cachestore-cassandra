package org.infinispan.persistence.cassandra;

import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.AbstractInfinispanTest;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.wait.CassandraQueryWaitStrategy;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;

@Test(groups = "unit", testName = "persistence.cassandra.CassandraConfigurationTest")
public class CassandraConfigurationTest extends AbstractInfinispanTest {
   private static CassandraContainer cassandraContainer;

   @BeforeClass
   public static void setup() throws PersistenceException {
      cassandraContainer = new FixedHostPortCassandraContainer<>()
              .withFixedExposedPort(9042, 9042)
              .withEnv("CASSANDRA_DC", "dc1")
              .withEnv("CASSANDRA_ENDPOINT_SNITCH", "GossipingPropertyFileSnitch")
              .waitingFor(new CassandraQueryWaitStrategy());
      cassandraContainer.start();
   }

   @AfterClass
   public static void teardown() {
      cassandraContainer.stop();
   }

   public void testXmlConfig() throws IOException {
      EmbeddedCacheManager cacheManager = new DefaultCacheManager("config.xml");
      Cache<String, String> cache = cacheManager.getCache("cassandracache");
      cache.put("Hello", "Moon");
      assertEquals(cache.get("Hello"), "Moon");
      cache.stop();
      cacheManager.stop();
   }

   public void testCustomStoreConfig() throws IOException {
      EmbeddedCacheManager cacheManager = new DefaultCacheManager("customstore-config.xml");
      Cache<String, String> cache = cacheManager.getCache("cassandracache");
      cache.put("Hi", "there");
      assertEquals(cache.get("Hi"), "there");
      cache.stop();
      cacheManager.stop();
   }

}
