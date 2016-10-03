package org.infinispan.persistence.cassandra.configuration;

import com.datastax.driver.core.ConsistencyLevel;
import org.infinispan.configuration.cache.AbstractStoreConfigurationChildBuilder;

public abstract class AbstractCassandraStoreConfigurationChildBuilder<S> extends AbstractStoreConfigurationChildBuilder<S>
      implements CassandraStoreConfigurationChildBuilder<S> {

   private final CassandraStoreConfigurationBuilder builder;

   protected AbstractCassandraStoreConfigurationChildBuilder(CassandraStoreConfigurationBuilder builder) {
      super(builder);
      this.builder = builder;
   }

   @Override
   public CassandraStoreServerConfigurationBuilder addServer() {
      return builder.addServer();
   }

   @Override
   public CassandraStoreConnectionPoolConfigurationBuilder connectionPool() {
      return builder.connectionPool();
   }

   @Override
   public CassandraStoreConfigurationBuilder autoCreateKeyspace(boolean autoCreateKeyspace) {
      return builder.autoCreateKeyspace(autoCreateKeyspace);
   }

   @Override
   public CassandraStoreConfigurationBuilder entryTable(String entryTable) {
      return builder.entryTable(entryTable);
   }

   @Override
   public CassandraStoreConfigurationBuilder keyspace(String keyspace) {
      return builder.keyspace(keyspace);
   }

   @Override
   public CassandraStoreConfigurationBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {
      return builder.consistencyLevel(consistencyLevel);
   }

   @Override
   public CassandraStoreConfigurationBuilder serialConsistencyLevel(ConsistencyLevel serialConsistencyLevel) {
      return builder.serialConsistencyLevel(serialConsistencyLevel);
   }

   @Override
   public CassandraStoreConfigurationBuilder replicationStrategy(String replicationStrategy) {
      return builder.replicationStrategy(replicationStrategy);
   }

}