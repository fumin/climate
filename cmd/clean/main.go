package main

import (
	"bytes"
	"cmp"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type RawDatum struct {
	t     time.Time
	empty bool
	v     float64
}

// https://nsidc.org/arcticseaicenews/sea-ice-tools/
func readOkhotsk(fpath string) ([]RawDatum, error) {
	f, err := os.Open(fpath)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	defer f.Close()
	r := csv.NewReader(f)

	// Header.
	if _, err := r.Read(); err != nil {
		return nil, errors.Wrap(err, "")
	}

	data := make([]RawDatum, 0)
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
			d := RawDatum{t: t, empty: true}

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

	slices.SortFunc(data, func(a, b RawDatum) int { return cmp.Compare(a.t.Unix(), b.t.Unix()) })
	return data, nil
}

// https://github.com/Raingel/historical_weather
func readTaiwan(fpath string) ([]RawDatum, error) {
	f, err := os.Open(fpath)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	defer f.Close()
	r := csv.NewReader(f)

	// Header.
	if _, err := r.Read(); err != nil {
		return nil, errors.Wrap(err, "")
	}

	data := make([]RawDatum, 0)
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

		t, err := time.Parse("2006-01-02", row[0])
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("%d", i))
		}
		v, err := strconv.ParseFloat(row[7], 64)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("%d", i))
		}

		d := RawDatum{t: t, v: v}
		data = append(data, d)
	}

	return data, nil
}

// https://www.data.jma.go.jp/gmd/risk/obsdl/index.php
func readJapan(fname string) ([]RawDatum, error) {
	f, err := os.Open(fname)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	defer f.Close()
	r := csv.NewReader(f)

	// Header.
	if _, err := r.Read(); err != nil {
		return nil, errors.Wrap(err, "")
	}

	data := make([]RawDatum, 0)
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

		t, err := time.Parse("1/2/2006", row[0])
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("%d", i))
		}
		d := RawDatum{t: t, empty: true}

		if row[1] != "" {
			d.v, err = strconv.ParseFloat(row[1], 64)
			if err != nil {
				return nil, errors.Wrap(err, fmt.Sprintf("%d", i))
			}
			d.empty = false
		}

		data = append(data, d)
	}

	return data, nil
}

// https://www.ncei.noaa.gov/access/search/data-search/global-summary-of-the-day
func readGSOD(fname string) ([]RawDatum, error) {
	f, err := os.Open(fname)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	defer f.Close()
	r := csv.NewReader(f)

	// Header.
	if _, err := r.Read(); err != nil {
		return nil, errors.Wrap(err, "")
	}

	data := make([]RawDatum, 0)
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

		t, err := time.Parse("2006-01-02", row[1])
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("%d", i))
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(row[2]), 64)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("%d", i))
		}
		v = (v - 32) * 5 / 9 // fahrenheit to celsius
		d := RawDatum{t: t, v: v}

		data = append(data, d)
	}

	return data, nil
}

type Datum struct {
	t        time.Time
	danshui  float64
	okhotsk  float64
	katsuura float64
	nemuro   float64
	yelizovo float64
}

func write(dst string, data []Datum) error {
	b := bytes.NewBuffer(nil)
	w := csv.NewWriter(b)
	row := []string{"t", "danshui", "okhotsk", "katsuura", "nemuro", "yelizovo"}
	if err := w.Write(row); err != nil {
		return errors.Wrap(err, "")
	}

	for _, d := range data {
		row[0] = d.t.Format(time.DateOnly)
		row[1] = strconv.FormatFloat(d.danshui, 'f', -1, 64)
		row[2] = strconv.FormatFloat(d.okhotsk, 'f', -1, 64)
		row[3] = strconv.FormatFloat(d.katsuura, 'f', -1, 64)
		row[4] = strconv.FormatFloat(d.nemuro, 'f', -1, 64)
		row[5] = strconv.FormatFloat(d.yelizovo, 'f', 1, 64)
		if err := w.Write(row); err != nil {
			return errors.Wrap(err, "")
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return errors.Wrap(err, "")
	}

	if err := os.WriteFile(dst, b.Bytes(), os.ModePerm); err != nil {
		return errors.Wrap(err, "")
	}
	return nil
}

func main() {
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Llongfile)
	if err := mainWithErr(); err != nil {
		log.Fatalf("%+v", err)
	}
}

func mainWithErr() error {
	okhotsk, err := readOkhotsk("data/okhotsk.csv")
	if err != nil {
		return errors.Wrap(err, "")
	}
	danshui, err := readTaiwan("data/danshui.csv")
	if err != nil {
		return errors.Wrap(err, "")
	}
	katsuura, err := readJapan("data/katsuura.csv")
	if err != nil {
		return errors.Wrap(err, "")
	}
	nemuro, err := readJapan("data/nemuro.csv")
	if err != nil {
		return errors.Wrap(err, "")
	}
	yelizovo, err := readGSOD("data/yelizovo.csv")
	if err != nil {
		return errors.Wrap(err, "")
	}

	okhotskM := make(map[string]RawDatum, len(okhotsk))
	for _, d := range okhotsk {
		s := d.t.Format(time.DateOnly)
		okhotskM[s] = d
	}
	danshuiM := make(map[string]struct{}, len(danshui))
	katsuuraM := make(map[string]RawDatum, len(katsuura))
	for _, d := range katsuura {
		s := d.t.Format(time.DateOnly)
		katsuuraM[s] = d
	}
	nemuroM := make(map[string]RawDatum, len(nemuro))
	for _, d := range nemuro {
		s := d.t.Format(time.DateOnly)
		nemuroM[s] = d
	}
	yelizovoM := make(map[string]RawDatum, len(yelizovo))
	for _, d := range yelizovo {
		s := d.t.Format(time.DateOnly)
		yelizovoM[s] = d
	}
	joined := make([]Datum, 0, len(danshui))
	for _, d := range danshui {
		s := d.t.Format(time.DateOnly)
		// Ignore duplicate rows in danshui.
		if _, ok := danshuiM[s]; ok {
			continue
		}
		danshuiM[s] = struct{}{}

		// Ignore data without okhotsk.
		od, ok := okhotskM[s]
		if !ok || od.empty {
			continue
		}

		// Ignore data without katsuura and nemuro.
		kd, ok := katsuuraM[s]
		if !ok || kd.empty {
			continue
		}
		nd, ok := nemuroM[s]
		if !ok || nd.empty {
			continue
		}

		yd, ok := yelizovoM[s]
		if !ok {
			continue
		}

		//Ignore outlier temperatures.
		if d.v < -90 {
			continue
		}

		joined = append(joined, Datum{t: d.t, danshui: d.v, okhotsk: od.v, katsuura: kd.v, nemuro: nd.v, yelizovo: yd.v})
	}

	if err := write("data.csv", joined); err != nil {
		return errors.Wrap(err, "")
	}
	return nil
}
