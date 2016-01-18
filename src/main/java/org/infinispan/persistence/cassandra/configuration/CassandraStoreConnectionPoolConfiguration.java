package org.infinispan.persistence.cassandra.configuration;

public class CassandraStoreConnectionPoolConfiguration {

   private int poolTimeoutMillis;
   private int heartbeatIntervalSeconds;
   private int idleTimeoutSeconds;

   CassandraStoreConnectionPoolConfiguration(int poolTimeoutMillis, int heartbeatIntervalSeconds, int idleTimeoutSeconds) {
      this.poolTimeoutMillis = poolTimeoutMillis;
      this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
      this.idleTimeoutSeconds = idleTimeoutSeconds;
   }

   public int poolTimeoutMillis() {
      return poolTimeoutMillis;
   }

   public int heartbeatIntervalSeconds() {
      return heartbeatIntervalSeconds;
   }

   public int idleTimeoutSeconds() {
      return idleTimeoutSeconds;
   }

   @Override
   public String toString() {
      return "CassandraStoreConnectionPoolConfiguration{" +
            "poolTimeoutMillis=" + poolTimeoutMillis +
            ", heartbeatIntervalSeconds=" + heartbeatIntervalSeconds +
            ", idleTimeoutSeconds=" + idleTimeoutSeconds +
            '}';
   }
}
