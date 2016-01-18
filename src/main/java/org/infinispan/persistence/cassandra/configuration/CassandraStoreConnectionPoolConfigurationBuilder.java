package org.infinispan.persistence.cassandra.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.global.GlobalConfiguration;

public class CassandraStoreConnectionPoolConfigurationBuilder extends AbstractCassandraStoreConfigurationChildBuilder<CassandraStoreConfigurationBuilder>
      implements Builder<CassandraStoreConnectionPoolConfiguration> {

   private int poolTimeoutMillis = 5;
   private int heartbeatIntervalSeconds = 30;
   private int idleTimeoutSeconds = 120;

   CassandraStoreConnectionPoolConfigurationBuilder(CassandraStoreConfigurationBuilder builder) {
      super(builder);
   }

   public CassandraStoreConnectionPoolConfigurationBuilder poolTimeoutMillis(int poolTimeoutMillis) {
      this.poolTimeoutMillis = poolTimeoutMillis;
      return this;
   }

   public CassandraStoreConnectionPoolConfigurationBuilder heartbeatIntervalSeconds(int heartbeatIntervalSeconds) {
      this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
      return this;
   }

   public CassandraStoreConnectionPoolConfigurationBuilder idleTimeoutSeconds(int idleTimeoutSeconds) {
      this.idleTimeoutSeconds = idleTimeoutSeconds;
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public CassandraStoreConnectionPoolConfiguration create() {
      return new CassandraStoreConnectionPoolConfiguration(poolTimeoutMillis, heartbeatIntervalSeconds, idleTimeoutSeconds);
   }

   @Override
   public CassandraStoreConnectionPoolConfigurationBuilder read(CassandraStoreConnectionPoolConfiguration template) {
      poolTimeoutMillis = template.poolTimeoutMillis();
      heartbeatIntervalSeconds = template.heartbeatIntervalSeconds();
      idleTimeoutSeconds = template.idleTimeoutSeconds();
      return this;
   }

}
