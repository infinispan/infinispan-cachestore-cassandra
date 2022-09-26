package org.infinispan.persistence.cassandra.configuration;

public class CassandraStoreConnectionPoolConfiguration {

   private int localSize;
   private int remoteSize;
   private int heartbeatIntervalSeconds;

   private int heartbeatTimeoutMs;

   CassandraStoreConnectionPoolConfiguration(int localSize, int remoteSize, int heartbeatIntervalSeconds, int heartbeatTimeoutMs) {
      this.localSize = localSize;
      this.remoteSize = remoteSize;
      this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
      this.heartbeatTimeoutMs = heartbeatTimeoutMs;
   }

   public int localSize() {
      return localSize;
   }

   public int remoteSize() {
      return remoteSize;
   }

   public int heartbeatIntervalSeconds() {
      return heartbeatIntervalSeconds;
   }

   public int heartbeatTimeoutMs() {
      return heartbeatTimeoutMs;
   }

   @Override
   public String toString() {
      return "CassandraStoreConnectionPoolConfiguration{" +
              "localSize=" + localSize +
              ", remoteSize=" + remoteSize +
              ", heartbeatIntervalSeconds=" + heartbeatIntervalSeconds +
              ", heartbeatTimeoutMs=" + heartbeatTimeoutMs +
              '}';
   }
}
