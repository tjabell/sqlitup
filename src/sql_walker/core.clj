(ns sql-walker.core
  (:require [clojure.string :as s]
            [clojure.java.jdbc :as j]
            [sql-walker.config :as config]))

(def db config/db)

(defn q
  [query]
  (j/query db query))

(defn s
  [t]
  (q (str "select * from " t)))

(defn ist [t]
  (def is "INFORMATION_SCHEMA")
  (str is "." t))

(def tc  (ist "TABLE_CONSTRAINTS"))
(def ccu (ist "CONSTRAINT_COLUMN_USAGE"))
(def rc  (ist "REFERENTIAL_CONSTRAINTS"))

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))
