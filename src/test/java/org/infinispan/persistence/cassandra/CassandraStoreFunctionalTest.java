package org.infinispan.persistence.cassandra;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.persistence.cassandra.configuration.CassandraStoreConfigurationBuilder;
import org.infinispan.persistence.spi.PersistenceException;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.wait.CassandraQueryWaitStrategy;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(testName = "CassandraStoreFunctionalTest", groups = "functional")
public class CassandraStoreFunctionalTest extends BaseStoreFunctionalTest {

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

    @Override
    protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence, String cacheName, boolean preload) {
        CassandraStoreConfigurationBuilder cfg = persistence.addStore(CassandraStoreConfigurationBuilder.class).preload(preload);
        cfg.segmented(false);
        cfg.autoCreateKeyspace(true);
        cfg.localDatacenter("dc1");
        cfg.addServer().host("localhost").port(cassandraContainer.getMappedPort(9042));
        return persistence;
    }

    @Test(enabled = false)
    @Override
    public void testTwoCachesSameCacheStore() {
        // not applicable, the caches need different configurations
    }
}
