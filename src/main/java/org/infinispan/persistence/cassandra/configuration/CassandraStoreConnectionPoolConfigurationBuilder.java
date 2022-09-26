package org.infinispan.persistence.cassandra.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.global.GlobalConfiguration;

public class CassandraStoreConnectionPoolConfigurationBuilder extends AbstractCassandraStoreConfigurationChildBuilder<CassandraStoreConfigurationBuilder>
      implements Builder<CassandraStoreConnectionPoolConfiguration> {

   private int localSize = 1;
   private int remoteSize = 1;
   private int heartbeatIntervalSeconds = 30;

   private int heartbeatTimeoutMs = 500;

   CassandraStoreConnectionPoolConfigurationBuilder(CassandraStoreConfigurationBuilder builder) {
      super(builder);
   }

   public CassandraStoreConnectionPoolConfigurationBuilder localSize(int localSize) {
      this.localSize = localSize;
      return this;
   }

   public CassandraStoreConnectionPoolConfigurationBuilder remoteSize(int remoteSize) {
      this.remoteSize = remoteSize;
      return this;
   }

   public CassandraStoreConnectionPoolConfigurationBuilder heartbeatIntervalSeconds(int heartbeatIntervalSeconds) {
      this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
      return this;
   }

   public CassandraStoreConnectionPoolConfigurationBuilder heartbeatTimeoutMs(int heartbeatTimeoutMs) {
      this.heartbeatTimeoutMs = heartbeatTimeoutMs;
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
      return new CassandraStoreConnectionPoolConfiguration(localSize, remoteSize, heartbeatIntervalSeconds, heartbeatTimeoutMs);
   }

   @Override
   public CassandraStoreConnectionPoolConfigurationBuilder read(CassandraStoreConnectionPoolConfiguration template) {
      localSize = template.localSize();
      remoteSize = template.remoteSize();
      heartbeatIntervalSeconds = template.heartbeatIntervalSeconds();
      heartbeatTimeoutMs = template.heartbeatTimeoutMs();
      return this;
   }

}
