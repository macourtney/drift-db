(ns drift-db.protocol)

(defprotocol Flavor
  "A protocol for the database flavors. This protocol includes all functions used by Conjure to interface with the
   database. To add a datbase, simply implement this protocol for the database and set the instance in
   config.db-config."

  (db-map [flavor] "Returns a map for use in db-config.")

  (execute-query [flavor sql-vector] "Executes an sql string and returns the results as a sequence of maps.")

  (execute-commands [flavor sql-strings] "Executes multiple sql strings without returning results.")

  (sql-find [flavor select-map]
    "Runs an sql select statement built from the given select-map. The valid keys are:

        table - the table to run the select statement on
        select - the columns to return. Either a string, or a vector of column names.
        where - the conditions as a string, vector, or prototype record")

  (create-table [flavor table specs]
    "Creates a new table with the given name and with columns based on the given specs.")

  (drop-table [flavor table] "Deletes the table with the given name.")

  (table-exists? [flavor table] "Returns true if the table with the given name exists.")

  (describe-table [flavor table]
    "Shows the columns of the given table. The result is a map which looks like:

       { :name <table name>
         :columns <columns specs> }
    
    Each column spec is exactly like the column spec passed into create table.")

  (add-column [flavor table spec]
    "Adds a column described by spec to the given table. Spec is a map describing a column.")

  (drop-column [flavor table column] "Removes the given column from the given table.")

  (format-date [flavor date] "Returns the string value of the given date for use in the database.")

  (format-date-time [flavor date] "Returns the string value of the given date as a date time for use in the database.")

  (format-time [flavor date] "Returns the string value of the given date as a time for use in the database.")

  (insert-into [flavor table records]
    "Inserts the given records into the given table.

      table - The name of the table to update.
      records - A map from strings or keywords (identifying columns) to updated values.")

  (delete [flavor table where-or-record] "Deletes rows from the table which satisfies the given where or prototype record.")

  (update [flavor table where-or-record record]
    "Runs an update given the table, where-params and a record.

      table - The name of the table to update.
      where-or-record - The where clause or a prototype record.
      record - A map from strings or keywords (identifying columns) to updated values."))