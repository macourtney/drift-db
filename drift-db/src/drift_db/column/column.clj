(ns drift-db.column.column
  (:require [clojure.tools.logging :as logging]))

(defprotocol Column
  (type [column] "Returns the column type. For example, :string, :integer or :date.")

  (name [column] "Returns the name of the column.")

  (length [column] "Returns the length of the column or nil if the column does not support length.")

  (precision [column] "Returns the precision of the column or nil if the column does not support precision.")

  (scale [column] "Returns the scale of the column or nil if the column does not support scale.")

  (nullable? [column] "Returns true if the column can be null, false otherwise. If the column does not support this option, then the column should return true.")

  (primary-key? [column] "Returns true if the column is a primary key, false otherwise. If the column does not support this option then this function should return false.")

  (auto-increment? [column] "Returns true if the column should automatically increment, false otherwise. If the column does not support this option then this function should return false."))