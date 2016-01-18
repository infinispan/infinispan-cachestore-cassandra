package org.infinispan.persistence.cassandra;

import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;

@Test(groups = "unit", testName = "persistence.cassandra.CassandraConfigurationTest")
public class CassandraConfigurationTest extends AbstractInfinispanTest {

   private EmbeddedCacheManager cacheManager;

   @BeforeClass
   public static void setup() throws PersistenceException, IOException {
      SingleEmbeddedCassandraService.start();
   }

   public void testXmlConfig() throws IOException {
      EmbeddedCacheManager cacheManager = new DefaultCacheManager("config.xml");
      Cache<String, String> cache = cacheManager.getCache("cassandracache");
      cache.put("Hello", "Moon");
      assertEquals(cache.get("Hello"), "Moon");
      cache.stop();
      cacheManager.stop();
   }

}