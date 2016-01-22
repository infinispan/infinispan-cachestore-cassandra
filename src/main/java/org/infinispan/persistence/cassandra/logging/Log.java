package org.infinispan.persistence.cassandra.logging;

import org.infinispan.persistence.spi.PersistenceException;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Log abstraction for the cassandra store. For this module, message ids ranging from 3001 to 4000 inclusively have been
 * reserved.
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {

   @Message(value = "Error communicating with Cassandra Store", id = 3001)
   PersistenceException errorCommunicating(@Cause Exception e);

   @Message(value = "Could not create a keyspace or entry table", id = 3002)
   PersistenceException errorCreatingKeyspace(@Cause Exception e);

   @Message(value = "Error writing an entry to Cassandra Store", id = 3003)
   PersistenceException errorWritingEntry(@Cause Exception e);

   @Message(value = "Error deleting an entry from Cassandra store", id = 3004)
   PersistenceException errorDeletingEntry(@Cause Exception e);

   @Message(value = "Error loading an entry from Cassandra store", id = 3005)
   PersistenceException errorLoadingEntry(@Cause Exception e);

   @Message(value = "Error when clearing Cassandra store", id = 3006)
   PersistenceException errorClearing(@Cause Exception e);

}