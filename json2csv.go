package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
)

var inFile = flag.String("in", "", "input file (defaults to stdin)")
var outFile = flag.String("out", "", "output file (defaults to stdout)")
var printCols = flag.Bool("cols", false, "print columns of first record and exit")

// var includeExtraColumns = flag.Bool("include-extra", false, "Include a catchall with extra columns")
// var errOnUnknownColumns = flag.Bool("error-on-unknown", false, "Error out on unknown columns")

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s <column.foo.bar> <column.foo.baz>\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(), "  Optional column names can be specified to limit the output columns, otherwise the columns from the first record will be used\n")
	}

	flag.Parse()

	columns := flag.Args()

	if *inFile == "" && len(columns) == 1 {
		inFile = &columns[0]
		columns = []string{}
	}

	var err error
	input := os.Stdin
	output := os.Stdout

	if *inFile != "" {
		input, err = os.Open(*inFile)
		if err != nil {
			log.Fatalf("Could not open input file: %s", err)
		}
		defer input.Close()
	} else {
		info, err := os.Stdin.Stat()
		if err == nil {
			mode := info.Mode()
			if mode&os.ModeCharDevice != 0 {
				fmt.Fprintf(os.Stderr, "Reading from stdin\n")
			}
		}
	}

	if *outFile != "" {
		output, err = os.Create(*outFile)
		if err != nil {
			log.Fatalf("Error creating output file: %s", err)
		}
	}

	dec := json.NewDecoder(input)
	enc := csv.NewWriter(output)

	if len(columns) == 0 {
		rec := make(map[string]interface{})
		err := dec.Decode(&rec)
		if err == io.EOF {
			return
		} else if err != nil {
			log.Fatalf("Error reading input: %s", err)
		}

		flatMap := make(map[string]string)
		flattenRecord("", rec, flatMap)

		keys := make([]string, 0, len(flatMap))
		for k := range flatMap {
			keys = append(keys, k)
		}

		sort.Strings(keys)

		if *printCols {
			for _, k := range keys {
				fmt.Println(k)
			}
			os.Exit(0)
		}

		enc.Write(keys)
		columns = keys
		printRecord(enc, columns, flatMap)
	} else {
		enc.Write(columns)
	}

	for {
		rec := make(map[string]interface{})
		err := dec.Decode(&rec)
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("Error reading input: %s", err)
		}

		flatMap := make(map[string]string)
		flattenRecord("", rec, flatMap)
		printRecord(enc, columns, flatMap)
	}
}

func printRecord(enc *csv.Writer, columns []string, flatMap map[string]string) {
	outCols := make([]string, len(columns))
	for i, colName := range columns {
		val := flatMap[colName]
		outCols[i] = val
	}
	enc.Write(outCols)
	enc.Flush()
}

func flattenRecord(prefix string, rec map[string]interface{}, destMap map[string]string) {
	for k, v := range rec {
		outKey := k
		if prefix != "" {
			outKey = prefix + "." + k
		}

		switch vv := v.(type) {
		case map[string]interface{}:
			flattenRecord(outKey, vv, destMap)
		case nil:
			destMap[outKey] = "null"
		case string:
			destMap[outKey] = vv
		case float64:
			destMap[outKey] = strconv.FormatFloat(vv, 'f', -1, 64)
		default:
			b, err := json.Marshal(v)
			if err != nil {
				panic(err)
			}
			destMap[outKey] = string(b)
		}
	}
}
