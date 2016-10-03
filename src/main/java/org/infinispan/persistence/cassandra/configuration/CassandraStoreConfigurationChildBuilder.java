package org.infinispan.persistence.cassandra.configuration;

import com.datastax.driver.core.ConsistencyLevel;
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

   /**
    * Consistency level to use for the read queries
    */
   CassandraStoreConfigurationBuilder readConsistencyLevel(ConsistencyLevel readConsistencyLevel);

   /**
    * Serial consistency level to use for the read queries
    */
   CassandraStoreConfigurationBuilder readSerialConsistencyLevel(ConsistencyLevel readSerialConsistencyLevel);

   /**
    * Consistency level to use for the write queries
    */
   CassandraStoreConfigurationBuilder writeConsistencyLevel(ConsistencyLevel writeConsistencyLevel);

   /**
    * Serial consistency level to use for the write queries
    */
   CassandraStoreConfigurationBuilder writeSerialConsistencyLevel(ConsistencyLevel writeSerialConsistencyLevel);

   /**
    * Replication strategy to use for the keyspace. Please see https://docs.datastax.com/en/cql/3.3/cql/cql_reference/create_keyspace_r.html on the format.
    */
   CassandraStoreConfigurationBuilder replicationStrategy(String replicationStrategy);

}
