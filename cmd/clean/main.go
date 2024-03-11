package main

import (
	"cmp"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

type Datum struct {
	t     time.Time
	empty bool
	v     float64
}

// https://nsidc.org/arcticseaicenews/sea-ice-tools/
func readOkhotsk() ([]Datum, error) {
	f, err := os.Open("okhotsk.csv")
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	defer f.Close()
	r := csv.NewReader(f)

	// Header.
	if _, err := r.Read(); err != nil {
		return nil, errors.Wrap(err, "")
	}

	data := make([]Datum, 0)
	var i int = 1
	for {
		i++
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("%d", i))
		}

		for col := 2; col < len(row); col++ {
			// Nonexistent February 29th.
			if row[0] == "2" && row[1] == "29" && row[col] == "" {
				continue
			}

			year := 1978 + col - 2
			tStr := fmt.Sprintf("%d-%s-%s", year, row[0], row[1])
			t, err := time.Parse("2006-1-2", tStr)
			if err != nil {
				return nil, errors.Wrap(err, fmt.Sprintf("%d %d", i, col))
			}
			d := Datum{t: t, empty: true}

			if row[col] != "" {
				d.v, err = strconv.ParseFloat(row[col], 64)
				if err != nil {
					return nil, errors.Wrap(err, fmt.Sprintf("%d %d", i, col))
				}
				d.empty = false
			}

			data = append(data, d)
		}
	}

	slices.SortFunc(data, func(a, b Datum) int { return cmp.Compare(a.t.Unix(), b.t.Unix()) })
	return data, nil
}

// https://github.com/Raingel/historical_weather
func readTaiwan() ([]Datum, error) {
	return nil, nil
}

func main() {
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Llongfile)
	if err := mainWithErr(); err != nil {
		log.Fatalf("%+v", err)
	}
}

func mainWithErr() error {
	okhotsk, err := readOkhotsk()
	if err != nil {
		return errors.Wrap(err, "")
	}
	log.Printf("%+v", okhotsk[500:503])
	return nil
}
