(ns drift-db.spec
  (:refer-clojure :exclude [type])
  (:require [clojure.tools.logging :as logging]))

(defprotocol Spec
  (type [column] "Returns the spec type. For example, :column."))
