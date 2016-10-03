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
   public CassandraStoreConfigurationBuilder readConsistencyLevel(ConsistencyLevel readConsistencyLevel) {
      return builder.readConsistencyLevel(readConsistencyLevel);
   }

   @Override
   public CassandraStoreConfigurationBuilder readSerialConsistencyLevel(ConsistencyLevel readSerialConsistencyLevel) {
      return builder.readSerialConsistencyLevel(readSerialConsistencyLevel);
   }

   @Override
   public CassandraStoreConfigurationBuilder writeConsistencyLevel(ConsistencyLevel writeConsistencyLevel) {
      return builder.writeConsistencyLevel(writeConsistencyLevel);
   }

   @Override
   public CassandraStoreConfigurationBuilder writeSerialConsistencyLevel(ConsistencyLevel writeSerialConsistencyLevel) {
      return builder.writeSerialConsistencyLevel(writeSerialConsistencyLevel);
   }

   @Override
   public CassandraStoreConfigurationBuilder replicationStrategy(String replicationStrategy) {
      return builder.replicationStrategy(replicationStrategy);
   }

}