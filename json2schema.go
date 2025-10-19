package json2schema

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	ac "github.com/apache/arrow-go/v18/arrow/csv"
)

var (
	ErrNoCsvRecordBatchGot error = errors.New("no csv record batch got")
	ErrKeyNotFound         error = errors.New("key not found")
)

type JsonRawObject []byte

type CsvForSchema struct {
	HeaderLine []byte
	Row1st     []byte
}

type JsonToCsvRaw func(JsonRawObject) (CsvForSchema, error)

type JsonMapObject map[string]any

type MapToHeaderLine func(JsonMapObject) ([]byte, error)
type MapToRow1st func(JsonMapObject) ([]byte, error)

type MapToCsv struct {
	MapToHeaderLine
	MapToRow1st
}

func (m MapToCsv) ToJsonToCsvRaw() JsonToCsvRaw {
	return func(j JsonRawObject) (CsvForSchema, error) {
		var empty CsvForSchema

		var jbytes []byte = j
		jmap := JsonMapObject{}

		e := json.Unmarshal(jbytes, &jmap)
		if nil != e {
			return empty, e
		}

		hline, ehdr := m.MapToHeaderLine(jmap)
		ro1st, ero1 := m.MapToRow1st(jmap)

		return CsvForSchema{
			HeaderLine: hline,
			Row1st:     ro1st,
		}, errors.Join(ehdr, ero1)
	}
}

type MapToHeaderStrings func(JsonMapObject) ([]string, error)
type MapToValueStrings func(JsonMapObject) ([]string, error)

func MapToHeaderStrsSorted(j JsonMapObject) ([]string, error) {
	var keys iter.Seq[string] = maps.Keys(j)
	var slc []string = slices.Collect(keys)
	slices.Sort(slc)
	return slc, nil
}

func val2str(val any) (string, error) {
	switch typedVal := val.(type) {
	case string:
		return typedVal, nil
	case float64:
		return fmt.Sprintf("%f", typedVal), nil
	case bool:
		return strconv.FormatBool(typedVal), nil
	default:
		var buf bytes.Buffer
		var enc *json.Encoder = json.NewEncoder(&buf)
		e := enc.Encode(val)
		if nil != e {
			return "", e
		}
		s := buf.String()
		return strings.TrimSpace(s), nil
	}
}

func MapToValueStrsSorted(jsonMap JsonMapObject) ([]string, error) {
	keys, e := MapToHeaderStrsSorted(jsonMap)
	if nil != e {
		return nil, e
	}

	vals := make([]string, 0, len(keys))
	for _, k := range keys {
		val, ok := jsonMap[k]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrKeyNotFound, k)
		}
		s, e := val2str(val)
		if nil != e {
			return nil, e
		}
		vals = append(vals, s)
	}

	return vals, nil
}

func (m MapToStrings) ToMapToRow1st() MapToRow1st {
	return func(j JsonMapObject) ([]byte, error) {
		vals, err := m.MapToValueStrings(j)
		if nil != err {
			return nil, err
		}

		var buf bytes.Buffer
		var wtr *csv.Writer = csv.NewWriter(&buf)
		err = wtr.Write(vals)
		if nil != err {
			return nil, err
		}

		wtr.Flush()
		if nil != wtr.Error() {
			return nil, wtr.Error()
		}

		return buf.Bytes(), nil
	}
}

type MapToStrings struct {
	MapToHeaderStrings
	MapToValueStrings
}

func (m MapToStrings) ToMapToHeaderLine() MapToHeaderLine {
	return func(j JsonMapObject) ([]byte, error) {
		keys, err := m.MapToHeaderStrings(j)
		if nil != err {
			return nil, err
		}

		var buf bytes.Buffer

		var wtr *csv.Writer = csv.NewWriter(&buf)

		err = wtr.Write(keys)
		if nil != err {
			return nil, err
		}

		wtr.Flush()
		if nil != wtr.Error() {
			return nil, wtr.Error()
		}

		return buf.Bytes(), nil
	}
}

type CsvForSchemaRow1stWithHeader []byte

func (c CsvForSchemaRow1stWithHeader) ToReader(opts ...ac.Option) *ac.Reader {
	var rdr io.Reader = bytes.NewReader(c)
	var allOpts []ac.Option = append([]ac.Option{ac.WithHeader(true)}, opts...)
	return ac.NewInferringReader(
		rdr,
		allOpts...,
	)
}

func (c CsvForSchemaRow1stWithHeader) ToSchema(
	opts ...ac.Option,
) (*arrow.Schema, error) {
	var rdr *ac.Reader = c.ToReader(opts...)
	defer rdr.Release()

	for rdr.Next() {
		rec := rdr.RecordBatch()
		return rec.Schema(), nil
	}

	return nil, ErrNoCsvRecordBatchGot
}
