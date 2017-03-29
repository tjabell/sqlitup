(ns sql-walker.core
  (:require [clojure.string :as s]
            [clojure.java.jdbc :as j]
            [sql-walker.config :as config]))

(def db config/db)

(defn q
  [query]
  (j/query db query))

(def query q)
(def qm
  (memoize q))
(defn s
  [t]
  (q (str "select * from " t)))

(defn ist [t]
  (def iss "INFORMATION_SCHEMA")
  (str iss "." t))

(def tc  (ist "TABLE_CONSTRAINTS"))
(def ccu (ist "CONSTRAINT_COLUMN_USAGE"))
(def rc  (ist "REFERENTIAL_CONSTRAINTS"))

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))

(def entity-tables '("AcdhhApplicationComment" "AcdhhAZTEDP_Applications" "AcdhhContacts" "AcdhhEquipment" "AcdhhEquipmentHistory" "AcdhhLicensure_ApplicationDeficiencies" "AcdhhLicensure_ApplicationFiles" "AcdhhLicensure_Applications" "AcdhhTransactionAdditionalReasons" "AcdhhTransactions" "AcdhhVouchers"))

(def query-get-foreign-key-sql
  (str
"    SELECT"
"    FK_Table = FK.TABLE_NAME,"
"    FK_Column = CU.COLUMN_NAME,"
"    FK_TYPE = FK.CONSTRAINT_TYPE,"
"    PK_Table = PK.TABLE_NAME,"
"    PK_Column = PT.COLUMN_NAME,"
"    Constraint_Name = C.CONSTRAINT_NAME"
" FROM"
"    INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS C"
" INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS FK"
"    ON C.CONSTRAINT_NAME = FK.CONSTRAINT_NAME"
" INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK"
"     ON C.UNIQUE_CONSTRAINT_NAME = PK.CONSTRAINT_NAME"
" INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE CU"
"     ON C.CONSTRAINT_NAME = CU.CONSTRAINT_NAME"
" INNER JOIN ("
"            SELECT"
"                i1.TABLE_NAME,"
"                i2.COLUMN_NAME"
"            FROM"
"                INFORMATION_SCHEMA.TABLE_CONSTRAINTS i1"
"            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE i2"
"                ON i1.CONSTRAINT_NAME = i2.CONSTRAINT_NAME"
"            WHERE"
"                i1.CONSTRAINT_TYPE = 'PRIMARY KEY'"
"           ) PT"
"    ON PT.TABLE_NAME = PK.TABLE_NAME"))

(defn query-get-constraints
  [t]
  (qm (str
      query-get-foreign-key-sql
      " WHERE FK.TABLE_NAME = '" t "'")))

(defn query-get-primary-key
  [t]
  (qm (str
       "select * from information_schema.table_constraints tc"
       " left join information_schema.key_column_usage cu"
       " on tc.constraint_name = cu.constraint_name"
       " where tc.table_name = '" t "'"
       " and constraint_type = 'PRIMARY KEY'")))

(defn query-get-columns
  [t]
  (qm
   (str
    "select column_name from information_schema.columns where table_name = '" t "'")))

(defn is-pk?
  [t c]
  (let [pks (query-get-primary-key t)]
    (.contains (map :column_name pks) (:column_name c))))

(defn is-fk?
  [t c]
  (let [fks (query-get-constraints t)]
    (.contains (map :fk_column fks) (:column_name  c))))

(defn retrieve-columns
  [table]
  (query (str "select data_type, column_name, numeric_scale, column_default from information_schema.columns where table_name = '" table "'")))

(defn is-entity?
  [t]
  (.contains entity-tables t))

(defn is-lookup?
  [t]
  (not (is-entity? t)))

(defn walk-database
  [t]
  (-> (query-get-columns t)
      (case (is-entity?))))

;; from https://stackoverflow.com/questions/925738/how-to-find-foreign-key-dependencies-in-sql-server
;;   SELECT
;;     FK_Table = FK.TABLE_NAME,
;;     FK_Column = CU.COLUMN_NAME,
;;     PK_Table = PK.TABLE_NAME,
;;     PK_Column = PT.COLUMN_NAME,
;;     Constraint_Name = C.CONSTRAINT_NAME
;; FROM
;;     INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS C
;; INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS FK
;;     ON C.CONSTRAINT_NAME = FK.CONSTRAINT_NAME
;; INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK
;;     ON C.UNIQUE_CONSTRAINT_NAME = PK.CONSTRAINT_NAME
;; INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE CU
;;     ON C.CONSTRAINT_NAME = CU.CONSTRAINT_NAME
;; INNER JOIN (
;;             SELECT
;;                 i1.TABLE_NAME,
;;                 i2.COLUMN_NAME
;;             FROM
;;                 INFORMATION_SCHEMA.TABLE_CONSTRAINTS i1
;;             INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE i2
;;                 ON i1.CONSTRAINT_NAME = i2.CONSTRAINT_NAME
;;             WHERE
;;                 i1.CONSTRAINT_TYPE = 'PRIMARY KEY'
;;            ) PT
;;     ON PT.TABLE_NAME = PK.TABLE_NAME
