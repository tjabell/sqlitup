(ns sql-walker.sql)

;; todo: clean up
(require '[clojure.core.reducers :as r]
         '[clojure.string :as s]
         '[clojure.java.jdbc :as jdbc]
         '[clojure.reflect :as reflect]
         '[clojure.data.xml :as x]
         '[clojure.data.generators :as gen]
         '[selmer.parser :as parser]
         '[selmer.util :refer [without-escaping]]

         '[camel-snake-kebab.core :as c]
         '[gen-mybatis.core.sql.generators :as sqlgen ])

(defn make-select
  ([cols] (make-select cols {}))
  ([cols config] 
   (let [col-specs (map (fn [col] (assoc config :col col)) cols)]
     (str "SELECT " (clojure.string/join ",
" (map (fn [col-spec] (parser/render select-column-template col-spec)) col-specs)) " "))))


(def get-pk-sql
  (str
   "SELECT i1.TABLE_SCHEMA, i1.CONSTRAINT_NAME, i1.TABLE_NAME, i2.COLUMN_NAME"
   " FROM"
   "     INFORMATION_SCHEMA.TABLE_CONSTRAINTS i1"
   " INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE i2"
   "     ON i1.CONSTRAINT_NAME = i2.CONSTRAINT_NAME"
   " WHERE"
   "     i1.CONSTRAINT_TYPE = 'PRIMARY KEY'"))

(defn get-pk
  [db s t]
  (jdbc/query db
              (str get-pk-sql
                   " AND i1.TABLE_SCHEMA = '" s "'"
                   " AND i1.TABLE_NAME = '" t "'")))
