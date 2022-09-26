package org.infinispan.persistence.cassandra;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.cassandra.configuration.CassandraStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ControlledTimeService;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.wait.CassandraQueryWaitStrategy;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(testName = "CassandraStoreTest", groups = "functional")
public class CassandraStoreTest extends BaseStoreTest {

   private static CassandraContainer cassandraContainer;

   @BeforeClass
   public static void setupStore() throws PersistenceException {
      cassandraContainer = new CassandraContainer<>()
              .withExposedPorts(9042)
              .withEnv("CASSANDRA_DC", "dc1")
              .withEnv("CASSANDRA_ENDPOINT_SNITCH", "GossipingPropertyFileSnitch")
              .waitingFor(new CassandraQueryWaitStrategy());
      cassandraContainer.start();
   }

   @AfterClass
   public static void teardownStore() {
      cassandraContainer.stop();
   }

   protected CassandraStoreConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder lcb) {
      CassandraStoreConfigurationBuilder cfg = lcb.addStore(CassandraStoreConfigurationBuilder.class).entryTable(this.getClass().getSimpleName());
      cfg.segmented(false);
      cfg.autoCreateKeyspace(true);
      cfg.localDatacenter("dc1");
      cfg.addServer().host("localhost").port(cassandraContainer.getMappedPort(9042));
      return cfg;
   }

   @Override
   protected ControlledTimeService getTimeService() {
      return new ControlledTimeService() {
         @Override
         public void advance(long time) {
            currentMillis += time;
            try {
               Thread.sleep(time); // we need to actually sleep, since cassandra runs on normal time
            } catch (InterruptedException e) {
               e.printStackTrace();
            }
         }
      };
   }

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {
      CassandraStore ccs = new CassandraStore();
      ConfigurationBuilder cb = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      createCacheStoreConfig(cb.persistence());
      ccs.init(createContext(cb.build()));
      return ccs;
   }

   @Override
   protected boolean storePurgesAllExpired() {
      return false;
   }

}
