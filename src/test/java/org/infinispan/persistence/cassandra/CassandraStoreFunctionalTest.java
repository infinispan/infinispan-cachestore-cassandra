package org.infinispan.persistence.cassandra;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.persistence.cassandra.configuration.CassandraStoreConfigurationBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(testName = "CassandraStoreFunctionalTest", groups = "functional")
public class CassandraStoreFunctionalTest extends BaseStoreFunctionalTest {

   @BeforeClass
   protected void setUp() throws Exception {
      SingleEmbeddedCassandraService.start();
   }

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence, boolean preload) {
      CassandraStoreConfigurationBuilder cfg = persistence.addStore(CassandraStoreConfigurationBuilder.class).preload(preload);
      cfg.autoCreateKeyspace(true);
      cfg.addServer().host("localhost");
      return persistence;
   }

   @Test(enabled = false)
   @Override
   public void testTwoCachesSameCacheStore() {
      // not applicable, the caches need different configurations
   }
}