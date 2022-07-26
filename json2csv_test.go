package main

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestFlattenRecord(t *testing.T) {
	rec := make(map[string]interface{})
	rec["urethrae-surgery"] = "knight-bedroll"
	subRec := make(map[string]interface{})
	subRec["metropolis-counterpanes"] = "anatomist-bankroll"
	subRec["distancing-sparklers"] = nil
	subRec["mineralogy-gladdening"] = 1
	subRec["slovenlier-communistic"] = 0.5
	subRec["armrest-garrison"] = []interface{}{"slims-Tudor", 1, 2, nil, "crossways-genomes"}
	rec["subrec"] = subRec

	destMap := make(map[string]string)
	flattenRecord("", rec, destMap)

	expect := map[string]string{
		"urethrae-surgery":               "knight-bedroll",
		"subrec.metropolis-counterpanes": "anatomist-bankroll",
		"subrec.distancing-sparklers":    "null",
		"subrec.mineralogy-gladdening":   "1",
		"subrec.slovenlier-communistic":  "0.5",
		"subrec.armrest-garrison":        `["slims-Tudor",1,2,null,"crossways-genomes"]`,
	}

	if !cmp.Equal(destMap, expect) {
		t.Fatal(cmp.Diff(destMap, expect))
	}
}
