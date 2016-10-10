package org.infinispan.persistence.cassandra;

import org.apache.cassandra.service.EmbeddedCassandraService;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.cassandra.configuration.CassandraStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ControlledTimeService;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

@Test(testName = "CassandraStoreTest", groups = "functional")
public class CassandraStoreTest extends BaseStoreTest {

   @BeforeClass
   public static void setup() throws PersistenceException, IOException {
      SingleEmbeddedCassandraService.start();
   }

   protected CassandraStoreConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder lcb) {
      CassandraStoreConfigurationBuilder cfg = lcb.addStore(CassandraStoreConfigurationBuilder.class).entryTable(this.getClass().getSimpleName());
      cfg.autoCreateKeyspace(true);
      cfg.addServer().host("localhost");
      return cfg;
   }

   @Override
   protected ControlledTimeService getTimeService() {
      return new ControlledTimeService(0) {
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
      EmbeddedCassandraService ecs = new EmbeddedCassandraService();
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
