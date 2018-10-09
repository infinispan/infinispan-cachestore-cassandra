package org.infinispan.persistence.cassandra;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.ParallelIterationTest;
import org.infinispan.persistence.cassandra.configuration.CassandraStoreConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author wburns
 * @since 10.0
 */
@Test(groups = "functional", testName = "persistence.cassandra.CassandraParallelIterationTest")
public class CassandraParallelIterationTest extends ParallelIterationTest {

   public void setup() throws Exception {
      SingleEmbeddedCassandraService.start();
      super.setup();
   }

   @Override
   protected void configurePersistence(ConfigurationBuilder cb) {
      cb.persistence().addStore(CassandraStoreConfigurationBuilder.class)
            .entryTable(this.getClass().getSimpleName())
            .autoCreateKeyspace(true)
            .addServer().host("localhost");
   }
}
