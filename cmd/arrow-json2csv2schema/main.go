package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	ac "github.com/apache/arrow-go/v18/arrow/csv"
	json2schema "github.com/takanoriyanagitani/go-arrow-json2csv2schema"
)

type SerializableField struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

type SerializableSchema struct {
	Fields []SerializableField `json:"fields"`
}

type cli struct {
	Input    io.ReadCloser
	Output   io.WriteCloser
	Args     []string
	ExitCode int
}

var ErrInvalidTypeMapping = errors.New("invalid type mapping")

func main() {
	cliApp := &cli{
		Input:    os.Stdin,
		Output:   os.Stdout,
		Args:     os.Args[1:],
		ExitCode: 0,
	}
	os.Exit(cliApp.run()) // exit with the code returned by cliApp.run()
}

const expectedTypeMappingParts = 2

func (cliApp *cli) run() int {
	pretty, includeColumns, columnTypes, inputFile, outputFile, err := cliApp.parseFlags()
	if err != nil {
		_, _ = fmt.Fprintln(cliApp.Output, err)
		return 1
	}

	err = cliApp.openInput(inputFile)
	if err != nil {
		_, _ = fmt.Fprintf(cliApp.Output, "Error opening input file: %v\n", err)
		return 1
	}
	defer func() {
		err := cliApp.Input.Close()
		if err != nil {
			_, _ = fmt.Fprintf(cliApp.Output, "Error closing input file: %v\n", err)
			cliApp.ExitCode = 1
		}
	}()

	err = cliApp.openOutput(outputFile)
	if err != nil {
		_, _ = fmt.Fprintf(cliApp.Output, "Error creating output file: %v\n", err)
		return 1
	}
	defer func() {
		err := cliApp.Output.Close()
		if err != nil {
			_, _ = fmt.Fprintf(cliApp.Output, "Error closing output file: %v\n", err)
			cliApp.ExitCode = 1
		}
	}()

	opts := createOptions(includeColumns, columnTypes)

	err = run(cliApp.Input, cliApp.Output, pretty, opts)
	if err != nil {
		_, _ = fmt.Fprintf(cliApp.Output, "Error: %v\n", err)
		return 1
	}

	return cliApp.ExitCode
}

func (cliApp *cli) parseFlags() (bool, []string, map[string]arrow.DataType, string, string, error) {
	var inputFile string
	var outputFile string
	var pretty bool
	var include string
	var types string

	flagSet := flag.NewFlagSet("arrow-json2csv2schema", flag.ContinueOnError)
	flagSet.StringVar(&inputFile, "input", "", "Input JSON file (default: stdin)")
	flagSet.StringVar(&inputFile, "i", "", "Input JSON file (shorthand)")
	flagSet.StringVar(&outputFile, "output", "", "Output file (default: stdout)")
	flagSet.StringVar(&outputFile, "o", "", "Output file (shorthand)")
	flagSet.BoolVar(&pretty, "pretty", false, "Pretty print the schema")
	flagSet.StringVar(&include, "include", "", "Comma-separated list of columns to include")
	flagSet.StringVar(&types, "types", "", "Comma-separated list of column_name:type pairs")

	err := flagSet.Parse(cliApp.Args)
	if err != nil {
		return false, nil, nil, "", "", err
	}

	includeColumns := []string{}
	if include != "" {
		includeColumns = strings.Split(include, ",")
	}

	columnTypes := make(map[string]arrow.DataType)
	if types != "" {
		pairs := strings.Split(types, ",")
		for _, pair := range pairs {
			kv := strings.Split(pair, ":")
			if len(kv) != expectedTypeMappingParts {
				return false, nil, nil, "", "", fmt.Errorf("%w: %s", ErrInvalidTypeMapping, pair)
			}
			columnTypes[kv[0]] = typeFromString(kv[1])
		}
	}

	return pretty, includeColumns, columnTypes, inputFile, outputFile, nil
}

func (cliApp *cli) openInput(inputFile string) error {
	if inputFile != "" {
		file, err := os.Open(inputFile) //nolint:gosec
		if err != nil {
			return err
		}
		cliApp.Input = file
	}
	return nil
}

func (cliApp *cli) openOutput(outputFile string) error {
	if outputFile != "" {
		file, err := os.Create(outputFile) //nolint:gosec
		if err != nil {
			return err
		}
		cliApp.Output = file
	}
	return nil
}

func createOptions(includeColumns []string, columnTypes map[string]arrow.DataType) []ac.Option {
	var opts []ac.Option
	if len(includeColumns) > 0 {
		opts = append(opts, ac.WithIncludeColumns(includeColumns))
	}
	if len(columnTypes) > 0 {
		opts = append(opts, ac.WithColumnTypes(columnTypes))
	}
	return opts
}

func typeFromString(s string) arrow.DataType {
	switch s {
	case "string":
		return arrow.BinaryTypes.String
	case "float64":
		return arrow.PrimitiveTypes.Float64
	case "bool":
		return arrow.FixedWidthTypes.Boolean
	default:
		return arrow.BinaryTypes.String
	}
}

func toSerializableSchema(schema *arrow.Schema) *SerializableSchema {
	serializableSchema := &SerializableSchema{
		Fields: make([]SerializableField, schema.NumFields()),
	}

	for idx, field := range schema.Fields() {
		switch field.Type.(type) {
		case *arrow.Float64Type:
			serializableSchema.Fields[idx] = SerializableField{
				Name:     field.Name,
				Type:     "float64",
				Nullable: field.Nullable,
			}
		case *arrow.StringType:
			serializableSchema.Fields[idx] = SerializableField{
				Name:     field.Name,
				Type:     "utf8",
				Nullable: field.Nullable,
			}
		case *arrow.BooleanType:
			serializableSchema.Fields[idx] = SerializableField{
				Name:     field.Name,
				Type:     "bool",
				Nullable: field.Nullable,
			}
		default:
			serializableSchema.Fields[idx] = SerializableField{
				Name:     field.Name,
				Type:     field.Type.Name(),
				Nullable: field.Nullable,
			}
		}
	}

	return serializableSchema
}

func run(r io.Reader, writer io.Writer, pretty bool, opts []ac.Option) error {
	jsonBytes, err := io.ReadAll(r)
	if nil != err {
		return err
	}

	var j2c json2schema.JsonToCsvRaw = json2schema.MapToCsv{
		MapToHeaderLine: json2schema.MapToStrings{
			MapToHeaderStrings: json2schema.MapToHeaderStrsSorted,
			MapToValueStrings:  json2schema.MapToValueStrsSorted,
		}.ToMapToHeaderLine(),
		MapToRow1st: json2schema.MapToStrings{
			MapToHeaderStrings: json2schema.MapToHeaderStrsSorted,
			MapToValueStrings:  json2schema.MapToValueStrsSorted,
		}.ToMapToRow1st(),
	}.ToJsonToCsvRaw()

	csvData, err := j2c(jsonBytes)
	if nil != err {
		return err
	}

	var buf bytes.Buffer
	_, err = buf.Write(csvData.HeaderLine)
	if nil != err {
		return err
	}

	_, err = buf.Write(csvData.Row1st)
	if nil != err {
		return err
	}

	schema, err := json2schema.CsvForSchemaRow1stWithHeader(buf.Bytes()).ToSchema(opts...)
	if nil != err {
		return err
	}

	if pretty {
		serializableSchema := toSerializableSchema(schema)
		jsonBytes, err := json.MarshalIndent(serializableSchema, "", "  ")
		if err != nil {
			return err
		}
		_, err = writer.Write(jsonBytes)
		return err
	}

	_, err = fmt.Fprintln(writer, schema)
	return err
}
