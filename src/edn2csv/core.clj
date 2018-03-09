(ns edn2csv.core
  (require [clojure.core.reducers :as r]
           [clojure.edn :as edn]
           [clojure.java.io :as io]
           [clojure.pprint :as pp]
           [iota]
           [me.raynes.fs :as fs])
  (:gen-class))

; The header line for the Individuals CSV file
(def individuals-header-line "UUID:ID(Individual),Generation:int,Location:int,:LABEL")
(def semantics-header-line "UUID:ID(Semantics),TotalError:int,:LABEL")
(def errors-header-line "UUID:ID(Error),ErrorValue:int,Position:int,:LABEL")
(def parentOf_edges-header-line ":START_ID(Individual),GeneticOperator,:END_ID(Individual),:TYPE")

(defn uuid [] (str (java.util.UUID/randomUUID)))
; Ignores (i.e., returns nil) any EDN entries that don't have the
; 'clojure/individual tag.
(defn individual-reader
    [t v]
    (when (= t 'clojush/individual) v))

; I got this from http://yellerapp.com/posts/2014-12-11-14-race-condition-in-clojure-println.html
; It prints in a way that avoids weird interleaving of lines and items.
; In several ways it would be better to use a CSV library like
; clojure.data.csv, but that won't (as written) avoid the interleaving
; problems, so I'm sticking with this approach for now.
(defn safe-println [output-stream & more]
  (.write output-stream (str (clojure.string/join "," more) "\n")))
; This prints out the relevant fields to the CSV filter
; and then returns 1 so we can count up how many individuals we processed.
; (The counting isn't strictly necessary, but it gives us something to
; fold together after we map this across the individuals; otherwise we'd
; just end up with a big list of nil's.)
(defn print-individual-to-csv
  [csv-file line]
  (as-> line $
    (map $ [:uuid :generation :location])
    (concat $ ["Individual"])
    (apply safe-println csv-file $))
  1)

(defn print-parent-of-edges-to-csv
  [csv-file line]
  (let [parents (get line :parent-uuids)]
    (dorun (map (fn [single-parent]
      (as-> line $
        (assoc $ :single-parent single-parent)
        (map $ [:single-parent :genetic-operators :uuid])
        (concat $ ["PARENT_OF"])
        (apply safe-println csv-file $))) parents))
  1))

(def a (atom #{}))

(defn print-semantics-to-csv
  [csv-file line]
  (let [semantics-uuid (uuid)
        values [semantics-uuid (get line :total-error) "Semantics"]]
      (dosync
      (if (compare-and-set! a @a @a)
        (if-not (contains? @a (get line :errors))
        (apply safe-println csv-file values)))
      ; (if-not (contains? @a :errors) ; (get line :errors))
      ;   (apply safe-println csv-file values))
      (swap! a conj (get line :errors)))
    1))

  (def b (atom #{}))

  (defn print-errors-to-csv
    [csv-file line]
    (let [errors-uuid (uuid)
          values [errors-uuid (get (get line :errors) :error-value) (get (get line :errors) :position) "Error"]]
          (dosync
            (if (compare-and-set! b @b @b)
              (if-not (contains? @b (get line :errors))
              (apply safe-println csv-file values)))
            (swap! b conj (get line :errors)))
          1))

;(defn edn->csv-sequential [edn-file csv-file]
  ;(with-open [out-file (io/writer csv-file)]
    ;(safe-println out-file individuals-header-line)
    ;(->>
      ;(line-seq (io/reader edn-file))
      ; Skip the first line because it's not an individual
      ;(drop 1)
      ;(map (partial edn/read-string {:default individual-reader}))
      ;(map (partial print-individual-to-csv out-file))
      ;(reduce +)
      ;)))

;(defn edn->csv-pmap [edn-file csv-file type]
  ;(with-open [out-file (io/writer csv-file)]
  ;(if type
    ;(str "individual" type)
    ;(safe-println out-file individuals-header-line)
    ;(->>
      ;(line-seq (io/reader edn-file))
      ; Skip the first line because it's not an individual
      ;(drop 1)
      ;(pmap (fn [line]
        ;(print-individual-to-csv out-file (edn/read-string {:default individual-reader} line))
        ;1))
    ;  count
    ;  )))
      ;ParentOf_Edge
    ;(safe-println out-file parentOf_edges-header-line)
    ;(->>
      ;(line-seq (io/reader ednvalues [semantics-uuid (get line :total-error) "Semantics"]]-file))
      ;(drop 1)
      ;(pmap (fn [line]
      ;  (print-parent-of-edges-to-csv out-file (edn/read-string {:default individual-reader} line))
      ;  1))
      ;  count
      ;  ))))

(defn edn->csv-reducers [edn-file csv-file]
  (with-open [out-file (io/writer csv-file)]
    (safe-println out-file errors-header-line)
    (->>
      (iota/seq edn-file)
      (r/map (partial edn/read-string {:default individual-reader}))
      ; This eliminates empty (nil) lines, which result whenever
      ; a line isn't a 'clojush/individual. That only happens on
      ; the first line, which is a 'clojush/run, but we still need
      ; to catch it. We could do that with `r/drop`, but that
      ; totally kills the parallelism. :-(
      (r/filter identity)
      (r/map (partial print-errors-to-csv out-file))
      (r/fold +)
      )))

(defn build-individual-csv-filename
  [edn-filename strategy]
  (str (fs/parent edn-filename)
       "/"
       (fs/base-name edn-filename ".edn")
       (if strategy
         (str "_" strategy)
         "_sequential")
       "_Individuals.csv"))

(defn build-parentOf-csv-filename
  [edn-filename strategy]
  (str (fs/parent edn-filename)
        "/"
        (fs/base-name edn-filename ".edn")
        ;(if strategy
          ;(str "_" strategy)
          ;"_sequential")
        "_ParentOf_edges.csv"))

(defn build-semantics-csv-filename
  [edn-filename strategy]
  (str (fs/parent edn-filename)
        "/"
        (fs/base-name edn-filename ".edn")
        ;(if strategy
          ;(str "_" strategy)
          ;"_sequential")
        "_Semantics.csv"))

(defn build-errors-csv-filename
    [edn-filename strategy]
    (str (fs/parent edn-filename)
          "/"
          (fs/base-name edn-filename ".edn")
                ;(if strategy
                  ;(str "_" strategy)
                  ;"_sequential")
          "_Errors.csv"))


(defn -main
  [edn-filename & [strategy]]
  (let [errors-csv-file (build-errors-csv-filename edn-filename strategy)]
    (time
      (condp = strategy
        ;"sequential" (edn->csv-sequential edn-filename individual-csv-file)
        ;"pmap" (edn->csv-pmap edn-filename individual-csv-file)
        ;"reducers" (edn->csv-reducers edn-filename individual-csv-file)
        (edn->csv-reducers edn-filename errors-csv-file))))
  ; Necessary to get threads spun up by `pmap` to shutdown so you get
  ; your prompt back right away when using `lein run`.
  (shutdown-agents))
