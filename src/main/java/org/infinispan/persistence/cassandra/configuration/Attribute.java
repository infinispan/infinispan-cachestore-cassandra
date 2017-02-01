package org.infinispan.persistence.cassandra.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Enumerates the attributes used by the Cassandra cache store configuration
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public enum Attribute {
   // must be first
   UNKNOWN(null),
   AUTO_CREATE_KEYSPACE("auto-create-keyspace"),
   KEYSPACE("keyspace"),
   ENTRY_TABLE("entry-table"),
   READ_CONSISTENCY_LEVEL("read-consistency-level"),
   READ_SERIAL_CONSISTENCY_LEVEL("read-serial-consistency-level"),
   WRITE_CONSISTENCY_LEVEL("write-consistency-level"),
   WRITE_SERIAL_CONSISTENCY_LEVEL("write-serial-consistency-level"),
   REPLICATION_STRATEGY("replication-strategy"),
   HOST("host"),
   PORT("port"),
   POOL_TIMEOUT_MILLIS("pool-timeout-millis"),
   HEARTBEAT_INTERVAL_SECONDS("heartbeat-interval-seconds"),
   IDLE_TIMEOUT_SECONDS("idle-timeout-seconds");

   private final String name;

   private Attribute(final String name) {
      this.name = name;
   }

   /**
    * Get the local name of this element.
    *
    * @return the local name
    */
   public String getLocalName() {
      return name;
   }

   private static final Map<String, Attribute> attributes;

   static {
      final Map<String, Attribute> map = new HashMap<String, Attribute>(64);
      for (Attribute attribute : values()) {
         final String name = attribute.getLocalName();
         if (name != null) {
            map.put(name, attribute);
         }
      }
      attributes = map;
   }

   public static Attribute forName(final String localName) {
      final Attribute attribute = attributes.get(localName);
      return attribute == null ? UNKNOWN : attribute;
   }
}
