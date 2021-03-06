(ns edn2csv.core
  (require [clojure.core.reducers :as r]
           [clojure.edn :as edn]
           [clojure.java.io :as io]
           [clojure.pprint :as pp]
           [iota]
           [me.raynes.fs :as fs])
  (:gen-class))

; The header lines for the CSV files
(def individuals-header-line "UUID:ID(Individual),Generation:int,Location:int,:LABEL")
(def semantics-header-line "UUID:ID(Semantics),TotalError:int,:LABEL")
(def errors-header-line "UUID:ID(Error),ErrorValue:int,Position:int,:LABEL")
(def parentOf_edges-header-line ":START_ID(Individual),GeneticOperator,:END_ID(Individual),:TYPE")
(def individual-semantics-edges-header-line ":START_ID(Individual),:END_ID(Semantics),:TYPE")
(def semantics-errors-header-line ":START_ID(Semantics),:END_ID(Error),:TYPE")

(def semantics-atom (atom #{}))
(def errors-atom (atom #{}))
(def individual-semantics-atom (atom #{}))
(def semantics-errors-atom (atom #{}))

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



(defn print-semantics-to-csv
  [csv-file line]
  (let [semantics-uuid (uuid)
        values [semantics-uuid (get line :total-error) "Semantics"]]
      (dosync
      (if (compare-and-set! semantics-atom @semantics-atom @semantics-atom)
        (if-not (contains? @semantics-atom (get line :errors))
        (apply safe-println csv-file values)))
      ; (if-not (contains? @a :errors) ; (get line :errors))
      ;   (apply safe-println csv-file values))
      (swap! semantics-atom conj (get line :errors)))
    1))


(defn print-errors-to-csv
    [csv-file line]
    (dosync
      (if (compare-and-set! errors-atom @errors-atom @errors-atom)
        (if-not (contains? @errors-atom (get line :errors))
          (doseq [x (get line :errors)]
          (let [error-uuid (uuid)
                values [error-uuid x (.indexOf (get line :errors) x) "Error"]]
                    (apply safe-println csv-file values)))))
            (swap! errors-atom conj (get line :errors)))
          1)



  (defn print-individual-semantic-edges-to-csv
    [csv-file line]
    (let [semantics-uuid (uuid)
          values [(get line :uuid) semantics-uuid "HAS_SEMANTICS"]]
        (dosync
          (if (compare-and-set! individual-semantics-atom @individual-semantics-atom @individual-semantics-atom)
            (if-not (contains? @individual-semantics-atom (get line :errors))
            (apply safe-println csv-file values)))
          (swap! individual-semantics-atom conj (get line :errors)))
          1))

  (defn print-semantics-errors-to-csv
    [csv-file line]
    (dosync
      (if (compare-and-set! semantics-errors-atom @semantics-errors-atom @semantics-errors-atom)
        (if-not (contains? @semantics-errors-atom (get line :errors))
          (doseq [x (get line :errors)]
          (let [error-uuid (uuid)
                semantics-uuid (uuid)
                values [semantics-uuid error-uuid "HAS_ERROR"]]
                    (apply safe-println csv-file values)))))
            (swap! semantics-errors-atom conj (get line :errors)))
          1)


  (defn edn->csv-individual [edn-file csv-file]
    (with-open [out-file (io/writer csv-file)]
    (safe-println out-file errors-header-line)
    (->>
      (iota/seq edn-file)
      (r/map (partial edn/read-string {:default individual-reader}))
      (r/filter identity)
      (r/map (partial print-individual-to-csv out-file))
      (r/fold +)
      )))

  (defn edn->csv-parentOf [edn-file csv-file]
    (with-open [out-file (io/writer csv-file)]
    (safe-println out-file errors-header-line)
    (->>
      (iota/seq edn-file)
      (r/map (partial edn/read-string {:default individual-reader}))
      (r/filter identity)
      (r/map (partial print-parent-of-edges-to-csv out-file))
      (r/fold +)
      )))

  (defn edn->csv-semantics [edn-file csv-file]
    (with-open [out-file (io/writer csv-file)]
    (safe-println out-file errors-header-line)
    (->>
      (iota/seq edn-file)
      (r/map (partial edn/read-string {:default individual-reader}))
      (r/filter identity)
      (r/map (partial print-semantics-to-csv out-file))
      (r/fold +)
      )))

  (defn edn->csv-errors [edn-file csv-file]
    (with-open [out-file (io/writer csv-file)]
    (safe-println out-file errors-header-line)
    (->>
      (iota/seq edn-file)
      (r/map (partial edn/read-string {:default individual-reader}))
      (r/filter identity)
      (r/map (partial print-errors-to-csv out-file))
      (r/fold +)
      )))

  (defn edn->csv-individual-semantics-edges [edn-file csv-file]
    (with-open [out-file (io/writer csv-file)]
    (safe-println out-file individual-semantics-edges-header-line)
    (->>
      (iota/seq edn-file)
      (r/map (partial edn/read-string {:default individual-reader}))
      (r/filter identity)
      (r/map (partial print-individual-semantic-edges-to-csv out-file))
      (r/fold +)
      )))

  (defn edn->csv-semantics-errors [edn-file csv-file]
    (with-open [out-file (io/writer csv-file)]
    (safe-println out-file semantics-errors-header-line)
    (->>
      (iota/seq edn-file)
      (r/map (partial edn/read-string {:default individual-reader}))
      (r/filter identity)
      (r/map (partial print-semantics-errors-to-csv out-file))
      (r/fold +)
      )))


(defn build-csv-filename
      [edn-filename strategy]
      (str (fs/parent edn-filename)
            "/"
            (fs/base-name edn-filename ".edn")
            (if strategy
              (str "_" strategy ".csv")
              "_individual.csv")))

; We used the same idea that was used when deciding which
; function to run but replaced the strategies with the type
; of file you want outputed, no startegy will output an "individual"
; csv file.
(defn -main
  [edn-filename & [strategy]]
  (let [csv-file (build-csv-filename edn-filename strategy)]
    (time
      (condp = strategy
        "individual" (edn->csv-individual edn-filename csv-file)
        "parentOfEdges" (edn->csv-parentOf edn-filename csv-file)
        "semantics" (edn->csv-semantics edn-filename csv-file)
        "errors" (edn->csv-errors edn-filename csv-file)
        "individual-semantics" (edn->csv-individual-semantics-edges edn-filename csv-file)
        "semantics-errors" (edn->csv-semantics-errors edn-filename csv-file)
        (edn->csv-individual edn-filename csv-file))))
  ; Necessary to get threads spun up by `pmap` to shutdown so you get
  ; your prompt back right away when using `lein run`.
  (shutdown-agents))
