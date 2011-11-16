(ns drift-db.protocol)

(defprotocol Flavor
  "A protocol for the database flavors. This protocol includes all functions used by Conjure to interface with the
   database. To add a datbase, simply implement this protocol for the database and set the instance in
   config.db-config."

  (db-map [flavor] "Returns a map for use in db-config.")

  (execute-query [flavor sql-vector] "Executes an sql string and returns the results as a sequence of maps.")

  (execute-commands [flavor sql-strings] "Executes an sql string without returning results.")

  (update [flavor table where-or-record record]
    "Runs an update given the table, where-params and a record.

      table - The name of the table to update.
      where-params - The parameters to test for.
      record - A map from strings or keywords (identifying columns) to updated values.")

  (insert-into [flavor table records]
    "Runs an insert given the table, and a set of records.

      table - The name of the table to update.
      records - A map from strings or keywords (identifying columns) to updated values.")

  (table-exists? [flavor table] "Returns true if the table with the given name exists.")

  (sql-find [flavor select-map]
    "Runs an sql select statement built from the given select-map. The valid keys are:

        table - the table to run the select statement on
        select - the columns to return
        where - the conditions as a string, vector, or prototype record")

  (create-table [flavor table specs]
    "Creates a new table with the given name and with columns based on the given specs.")

  (drop-table [flavor table] "Deletes the table with the given name.")

  (add-column [flavor table spec] "Adds a column described by spec to the given table.")

  (drop-column [flavor table column] "Removes the given column from the given table.")

  (describe-table [flavor table] "Shows the columns of the given table.")

  (delete [flavor table where-or-record] "Deletes rows from the table which satisfies the given where or prototype record.")

  (format-date [flavor date] "Returns the string value of the given date for use in the database.")

  (format-date-time [flavor date] "Returns the string value of the given date as a date time for use in the database.")

  (format-time [flavor date] "Returns the string value of the given date as a time for use in the database."))