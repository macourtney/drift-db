# drift-db

A database interface for use with drift.

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

### Describing a table.

You can describe a table using the `describe-table` function.

Example:

```clojure
  (describe-table :test)
```

The above example returns the description for the "test" table. The description is a map with two keys, the first is the :name which maps to the name of the table, the second is :columns which maps to a list of columns in the database. Each column description is similar in format to the spec map created by the column spec functions.

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

The above example drops the "added" column from the "test" table.

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

To delete a row, use the `delete` function.

Example:

```clojure
  (delete :test ["NAME = ?" "blah"])
```

The above example deletes the row with the name "blah".

There are a couple of ways to read rows out of the database. The two methods you can use are `sql-find` and `execute-query`.

Sql-find takes a map describing a select statement. The map supports the keys :table, :select, :where and possibly :limit.

Example:

```clojure
  (sql-find { :table :test :select "*" :where "NAME = 'blah'" :limit 1 })
```

The above example selects all columns from the "test" table where the name is "blah" limiting the results to only one row.

If you want to run an arbitrary sql query, you can use `execute-query`.

Example

```clojure
  (execute-query ["SELECT * FROM TEST WHERE NAME = ?" "blah"])
```

The above example selects all columns from test where name is equal to "blah". `execute-query` allows you to run sql statements directly on your database, the exact syntax for your database may differ.

## License

Copyright (c) 2011 Matthew Courtney and released under the Apache 2.0 license.
