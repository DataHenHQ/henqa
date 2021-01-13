package qa

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/DataHenHQ/datahen/records"
	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/xeipuuv/gojsonschema"
)

type RecordWrapper struct {
	Errors []records.SchemaError `json:"errors"`
	Record interface{}           `json:"record"`
}

type ErrorStat struct {
	Field            string  `json:"field"`
	ErrorType        string  `json:"error_type"`
	ErrorDescription string  `json:"error_description"`
	ErrorCount       uint64  `json:"error_count"`
	RecordCount      uint64  `json:"record_count"`
	ErrorPercent     float64 `json:"error_percent"`
}

type ErrorStats map[string]*ErrorStat

func (e *ErrorStat) IncErrCount() {
	e.ErrorCount = e.ErrorCount + 1
}

func (e *ErrorStat) CalculatePercentage() {
	e.ErrorPercent = float64(e.ErrorCount) / float64(e.RecordCount) * 100.0
}

type RecordsValidationResult struct {
	Collections map[string][]RecordWrapper         `json:"collections"`
	Stats       map[string]*records.CollectionStat `json:"stats"`
}

func Validate(ins []string, schemas []string, outDir string) (err error) {
	fmt.Println("validates the data in:", ins, "schemas:", schemas, "outDir:", outDir)

	files := getListOfFiles(ins)
	if len(files) > 0 {
		fmt.Println("input files are:")
	}
	for _, f := range files {
		fmt.Println(f)
	}

	mergedSchema, err := getAndMergeSchemaFiles(schemas)
	if err != nil {
		fmt.Println("gotten error with merging schemas:", err.Error())
		fmt.Println("aborting operation.")
	}
	// fmt.Println("merged Schema:")
	// fmt.Println(string(mergedSchema))

	err = validateWithSchema(files, mergedSchema, outDir)
	if err != nil {
		fmt.Println("gotten error running the validation:", err.Error())
	}

	return nil
}

func getListOfFiles(ins []string) (files []string) {
	for _, in := range ins {
		fmt.Println("checking in:", in)

		if isDir(in) {
			subDirFiles := getFilesFromDir(in)
			files = append(files, subDirFiles...)
			continue
		}

		if !fileExists(in) {
			fmt.Printf("file does not exist: %v\n", in)
			continue
		}
		fmt.Println("file exists:", in)

		files = append(files, in)
	}

	return uniqueStringSlice(files)
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func isDir(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return info.IsDir()
}

func getFilesFromDir(dir string) (files []string) {
	fs, _ := ioutil.ReadDir(dir)
	for _, f := range fs {
		if f.IsDir() {
			continue
		}

		files = append(files, filepath.Join(dir, f.Name()))

	}
	return files
}

func getAndMergeSchemaFiles(files []string) (schema []byte, err error) {

	for _, f := range files {
		nschema, err := ioutil.ReadFile(f)
		if err != nil {
			return nil, err
		}

		if schema == nil {
			schema = nschema
			continue
		}

		newSchema, err := jsonpatch.MergeMergePatches(schema, nschema)
		if err != nil {
			fmt.Print("cannot merge schema:", err.Error())
			return nil, err
		}

		schema = newSchema

	}
	return schema, nil
}

func validateWithSchema(files []string, schema []byte, outDir string) (err error) {

	schemaSl := gojsonschema.NewStringLoader(string(schema))
	sl := gojsonschema.NewSchemaLoader()
	sl.Validate = true
	err = sl.AddSchemas(schemaSl)
	if err != nil {
		return err
	}

	summaryErrStats := map[string]ErrorStats{}

	for _, f := range files {
		jsonB, err := ioutil.ReadFile(f)
		if err != nil {
			fmt.Println("gotten error reading ", f, ":", err.Error())
			continue
		}

		colrecs, _, err := records.PlainSchemaValidateFromJSON(string(schema), string(jsonB))

		errStats, err := writeValidationOutputs(outDir, f, colrecs)
		if err != nil {
			return err
		}

		basefile := filepath.Base(f)

		summaryErrStats[basefile] = errStats

	}

	if err := writeOverallSummaryFile(outDir, summaryErrStats); err != nil {
		return err
	}

	return nil
}

func createOutDirIfNotExist(path string) (err error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return os.Mkdir(path, os.ModeDir|0755)
	}
	return nil
}

func writeOverallSummaryFile(outDir string, summaryErrStats map[string]ErrorStats) (err error) {
	if err := createOutDirIfNotExist(outDir); err != nil {
		return err
	}

	// save summary data
	summaryData, err := json.MarshalIndent(summaryErrStats, "", " ")
	if err != nil {
		return err
	}
	summaryFile := filepath.Join(outDir, "summary.json")
	ioutil.WriteFile(summaryFile, summaryData, 0644)

	return nil
}

func writeValidationOutputs(outDir string, infilepath string, colrecs map[string][]records.RecordGetSetterWithError) (errStats map[string]*ErrorStat, err error) {
	if err := createOutDirIfNotExist(outDir); err != nil {
		return nil, err
	}

	infile := filepath.Base(infilepath)

	detailsDir := filepath.Join(outDir, "details")
	summaryDir := filepath.Join(outDir, "summary")
	createOutDirIfNotExist(detailsDir)
	createOutDirIfNotExist(summaryDir)

	recs := colrecs["default"]

	recordCount := len(recs)

	recWs := []RecordWrapper{}
	errStats = map[string]*ErrorStat{}

	for _, rec := range recs {
		o := records.TransformToRecordJSONB(rec)

		// delete any unused field from datahen's output records
		delete(o, "_collection")

		errs := rec.GetErrors()
		if errs == nil {
			continue
		}

		for _, e := range errs {
			errKey := fmt.Sprintf("%v.%v", e.Field, e.ErrorType)

			// if it doesn't exist then set a new record
			if errStats[errKey] == nil {
				es := ErrorStat{
					Field:            e.Field,
					ErrorType:        e.ErrorType,
					ErrorDescription: e.Description,
					ErrorCount:       1,
					RecordCount:      uint64(recordCount),
				}
				es.CalculatePercentage()
				errStats[errKey] = &es
				continue
			}

			errStats[errKey].IncErrCount()
			errStats[errKey].CalculatePercentage()

		}

		recWs = append(recWs, RecordWrapper{
			Errors: errs,
			Record: o,
		})
	}

	// save details data
	detailsData, err := json.MarshalIndent(recWs, "", " ")
	if err != nil {
		return nil, err
	}
	detailsFile := filepath.Join(detailsDir, infile)
	ioutil.WriteFile(detailsFile, detailsData, 0644)

	// save summary
	summaryData, err := json.MarshalIndent(errStats, "", " ")
	if err != nil {
		return nil, err
	}
	summaryFile := filepath.Join(summaryDir, infile)
	ioutil.WriteFile(summaryFile, summaryData, 0644)

	return errStats, nil
}
