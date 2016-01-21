package org.infinispan.persistence.cassandra.logging;

import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

import static org.jboss.logging.Logger.Level.ERROR;

/**
 * Log abstraction for the cassandra store. For this module, message ids ranging from 3001 to 4000 inclusively have been
 * reserved.
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {

   @LogMessage(level = ERROR)
   @Message(value = "Error communicating with Cassandra Store", id = 3001)
   void errorCommunicating(@Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Could not create a keyspace or entry table", id = 3002)
   void errorCreatingKeyspace(@Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Error writing an entry to Cassandra Store", id = 3003)
   void errorWritingEntry(@Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Error deleting an entry from Cassandra store", id = 3004)
   void errorDeletingEntry(@Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Error loading an entry from Cassandra store", id = 3005)
   void errorLoadingEntry(@Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Error when clearing Cassandra store", id = 3006)
   void errorClearing(@Cause Exception e);

}