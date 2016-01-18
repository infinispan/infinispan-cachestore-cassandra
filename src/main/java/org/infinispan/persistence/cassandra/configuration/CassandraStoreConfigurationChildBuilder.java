package org.infinispan.persistence.cassandra.configuration;

import org.infinispan.configuration.cache.StoreConfigurationChildBuilder;

public interface CassandraStoreConfigurationChildBuilder<S> extends StoreConfigurationChildBuilder<S> {

   /**
    * Adds a new cassandra server
    */
   CassandraStoreServerConfigurationBuilder addServer();

   /**
    * Configures the connection pool
    */
   CassandraStoreConnectionPoolConfigurationBuilder connectionPool();

   /**
    * Configures whether the cache store should automatically create the cassandra keyspace and entry table
    */
   CassandraStoreConfigurationBuilder autoCreateKeyspace(boolean autoCreateKeyspace);

   /**
    * Name of the table storing entries
    */
   CassandraStoreConfigurationBuilder entryTable(String entryTable);

   /**
    * Name of the keyspace which has the entry table
    */
   CassandraStoreConfigurationBuilder keyspace(String keyspace);

}
