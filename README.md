# drift-db

A database interface for use with drift.

## Including drift-db in your Leiningen project

To include drift-db in your Leiningen project, you first need to determine what database you will use. The two currently supported databases are H2 and Mysql. However, I hope more will be supported soon.

To include the H2 drift-db library, use:

```clojure
  [org.drift-db/drift-db-h2 "x.x.x"]
```

Where "x.x.x" is the current version of drift-db-h2. You can get the current version of drift-db-h2 from: http://clojars.org/org.drift-db/drift-db-h2

To include the Mysql drift-db library use:

```clojure
  [org.drift-db/drift-db-mysql "x.x.x"]
```

Where "x.x.x" is the current version of drift-db-mysql. You can get the current version of drift-db-mysql from: http://clojars.org/org.drift-db/drift-db-mysql

If you want to create your own implementation of the drift-db protocol, you can use:

```clojure
  [org.drift-db/drift-db "x.x.x"]
```

You can get the current version of drift-db from: http://clojars.org/org.drift-db/drift-db

## Usage

Drift db focuses on database tasks used in migrating databases. Creating, updating and deleting tables in a database are the primary tasks.

### Initializing drift db

Before you can do anything, you must let drift-db know which database flavor to use. To do this, simply call the init-flavor function.

```clojure
  (init-flavor flavor)
```

### Creating a table

To create a table use the `create-table`. Create table takes the name of the table to create and any number of specs for the table. Currently only column specs are supported.

Example:

```clojure
  (create-table :test
    (id :id)
    (string :name { :length 20 :not-null true :primary-key true }))
```

The above call to `create-table` creates a table with the name "test" and the columns "id" and "name". The id column is a special column which is basically an integer column which cannot be null, auto increments and is the primary key.

Drift db supports the following column functions:

  `date`
  `date-time`
  `integer`
  `id`
  `belongs-to`
  `decimal`
  `string`
  `text`
  `time-type`

### Dropping a table

To drop a table use the `drop-table` function:

Example:

```clojure
  (drop-table :test)
```

The above example drops the table with the name text.

### Testing for the existence of a table.

To test for the existence of a table, use the `table-exists?` function:

Example:

```clojure
  (table-exists? :test)
```

### Describing a table.

You can describe a table using the `describe-table` function.

Example:

```clojure
  (describe-table :test)
```

The above example returns the description for the "test" table. The description is a map with two keys, the first is the :name which maps to the name of the table, the second is :columns which maps to a list of columns in the database. Each column description is similar in format to the spec map created by the column spec functions.

You can get just the columns of a table using the `columns` function.

Example:

```clojure
  (columns :test)
```

The above example returns the columns from the table "test".

You can also pass the table map to `columns`.

Example:

```clojure
  (columns (describe-table :test))
```

The above example returns exactly the same results as the example before it.

You can find the specs for a column with `find-column`.

Example:

```clojure
  (find-column :test :added)
```

The above example returns the specs for the column "added" on the table "test". You can also pass a table map for the table name, and a column spec for the column name.

If you just want to know if a column exists in a table use `column-exists?`.

Example:

```clojure
  (column-exists? :test :added)
```

The above example returns a true value if the column "added" is in the table "test".

### Adding and dropping a column

You can add a column to an already existing table using the `add-column` function.

Example:

```clojure
  (add-column :test
    (string :added))
```

The above example adds the string column "added" to the table "test".

You can drop a column with the function `drop-column`.

Example:

```clojure
  (drop-column :test :added)
```

The above example drops the "added" column from the "test" table. If the "added" column does not exist on the "test" table, then the function will throw an exception.

You can also drop a column only if it exists using `drop-column-if-exists`.

Example:

```clojure
  (drop-column-if-exists :test :added)
```

The above example drops the "added" column from the "test" table if it exists. If it doesn't exist, the function does nothing.

### Create, Read, Update and Delete rows.

Though it is not the focus of drift-db, you can create, read, update and delete rows from tables.

To create (insert) rows into the database, use the `insert-into` function.

Example:

```clojure
  (insert-into :test { :name "blah" })
```

The above example inserts a row with the name "blah".

To update a row, use the update function.

Example:

```clojure
  (update :test ["NAME = ?" "blah"] { :name "blah2" })
```

The above example updates the row with the name "blah" and resets the name to "blah2".

Update can also take a prototype record for the where clause.

Example:

```clojure
  (update :test { :name "blah" } { :name "blah2" })
```

Or you can send a simple string as the where clause.

Example:

```clojure
  (update :test "NAME = blah" { :name "blah2" })
```

To delete a row, use the `delete` function.

Example:

```clojure
  (delete :test ["NAME = ?" "blah"])
```

The above example deletes the row with the name "blah".

Delete can also take a prototype record or string for the where clause just like update.

There are a couple of ways to read rows out of the database. The two methods you can use are `sql-find` and `execute-query`.

Sql-find takes a map describing a select statement. The map supports the keys :table, :select, :where and possibly :limit.

Example:

```clojure
  (sql-find { :table :test :select "*" :where "NAME = 'blah'" :limit 1 })
```

The above example selects all columns from the "test" table where the name is "blah" limiting the results to only one row.

You can also pass a vector for the where clause.

Example:

```clojure
  (sql-find { :table :test :select "*" :where ["NAME = ?" "blah"] :limit 1 })
```

Or, you can pass a prototype record for the where clause.

Example:

```clojure
  (sql-find { :table :test :select "*" :where { :name "blah" } :limit 1 })
```

Select can be a string or a vector of column names.

Example:

```clojure
  (sql-find { :table :test :select [:name :age] :where { :name "blah" } :limit 1 })
```

If you want to run an arbitrary sql query, you can use `execute-query`.

Example

```clojure
  (execute-query ["SELECT * FROM TEST WHERE NAME = ?" "blah"])
```

The above example selects all columns from test where name is equal to "blah". `execute-query` allows you to run sql statements directly on your database, the exact syntax for your database may differ.

## License

Copyright (c) 2011 Matthew Courtney and released under the Apache 2.0 license.
