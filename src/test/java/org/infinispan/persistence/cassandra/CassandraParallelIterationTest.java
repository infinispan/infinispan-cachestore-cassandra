package org.infinispan.persistence.cassandra;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.ParallelIterationTest;
import org.infinispan.persistence.cassandra.configuration.CassandraStoreConfigurationBuilder;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.wait.CassandraQueryWaitStrategy;
import org.testng.annotations.Test;

/**
 * @author wburns
 * @since 10.0
 */
@Test(groups = "functional", testName = "persistence.cassandra.CassandraParallelIterationTest")
public class CassandraParallelIterationTest extends ParallelIterationTest {
   private CassandraContainer cassandraContainer;

   @Override
   public void setup() throws Exception {
      cassandraContainer = new CassandraContainer<>()
              .withExposedPorts(9042)
              .withEnv("CASSANDRA_DC", "dc1")
              .withEnv("CASSANDRA_ENDPOINT_SNITCH", "GossipingPropertyFileSnitch")
              .waitingFor(new CassandraQueryWaitStrategy());
      cassandraContainer.start();
      super.setup();
   }

   @Override
   protected void teardown() {
      cassandraContainer.stop();
      super.teardown();
   }

   @Override
   protected void configurePersistence(ConfigurationBuilder cb) {
      cb.persistence().addStore(CassandraStoreConfigurationBuilder.class)
              .segmented(false)
              .entryTable(this.getClass().getSimpleName())
              .autoCreateKeyspace(true)
              .localDatacenter("dc1")
              .addServer().host("localhost").port(cassandraContainer.getMappedPort(9042));
   }
}
