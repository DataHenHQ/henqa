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

type processFileStreamFn func(string, int, records.ValidateFn) error

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
	// if len(files) > 0 {
	// 	fmt.Println("input files are:")
	// }
	// for _, f := range files {
	// 	fmt.Println(f)
	// }

	mergedSchema, err := getAndMergeSchemaFiles(schemas)
	if err != nil {
		fmt.Println("gotten error with merging schemas:", err.Error())
		fmt.Println("aborting validation.")
		return
	}
	// fmt.Println("merged Schema:")
	// fmt.Println(string(mergedSchema))

	// ensure output dir exists
	err = createOutDirIfNotExist(outDir)
	if err != nil {
		fmt.Println("gotten error creating output directory:", err.Error())
		fmt.Println("aborting validation.")
		return
	}

	err = validateWithSchema(files, mergedSchema, outDir)
	if err != nil {
		fmt.Println("gotten error running the validation:", err.Error())
		fmt.Println("aborting validation.")
		return
	}

	fmt.Println("Done validating record. The report folder would be located at", outDir)
	return nil
}

func getListOfFiles(ins []string) (files []string) {
	for _, in := range ins {

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
		subPath := filepath.Join(dir, f.Name())
		if f.IsDir() {
			subFiles := getFilesFromDir(subPath)
			for _, sf := range subFiles {
				files = append(files, sf)
			}
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
	schemaString := string(schema)
	schemaSl := gojsonschema.NewStringLoader(schemaString)
	sl := gojsonschema.NewSchemaLoader()
	sl.Validate = true
	err = sl.AddSchemas(schemaSl)
	if err != nil {
		return err
	}

	// build schema
	loader := gojsonschema.NewStringLoader(schemaString)
	colSchemaLoaders := make(map[string]*gojsonschema.JSONLoader)
	colSchemaLoaders["default"] = &loader

	summaryErrStats := map[string]ErrorStats{}
	batchSize := 100
	for _, f := range files {
		var processFile processFileStreamFn = nil

		switch filepath.Ext(f) {
		case ".csv":
			processFile = records.ProcessCSVFile
		case ".json":
			processFile = records.ProcessJSONFile
		default:
			fmt.Println(f, "is not a .csv or .json file. Skipping")
			continue
		}
		if err != nil {
			fmt.Println("gotten error reading ", f, ":", err.Error())
			continue
		}
		fmt.Println("validating:", f)

		// init detail file
		err = initDetailFile(outDir, f)
		if err != nil {
			fmt.Println("gotten error initializing output files for ", f, ":", err.Error())
			return err
		}

		// process the files and keep stats
		colstats := make(map[string]*records.CollectionStat)
		errStats := map[string]*ErrorStat{}
		var recordCount uint64 = 0
		isFirst := true
		processFile(f, batchSize, func(recs []records.RecordGetSetterWithError) (err2 error) {
			colrecs := records.SchemaLoadersValidate(colSchemaLoaders, recs, colstats)
			recordCount += uint64(len(colrecs["default"]))
			err2 = writeValidationOutputs(outDir, f, colrecs, errStats, isFirst)
			if err2 != nil {
				return err2
			}
			isFirst = false

			return nil
		})

		// close defail file
		err = closeDetailFile(outDir, f)
		if err != nil {
			fmt.Println("gotten error initializing output files for ", f, ":", err.Error())
			return err
		}

		// fix errStats record counters and write summary
		for _, es := range errStats {
			es.RecordCount = recordCount
			es.CalculatePercentage()
		}
		writeSummaryOutputs(outDir, f, errStats)

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

func initDetailFile(outDir string, infilepath string) (err error) {
	infile := filepath.Base(infilepath)

	// clear and init details file
	detailsDir := filepath.Join(outDir, "details")
	err = createOutDirIfNotExist(detailsDir)
	if err != nil {
		return err
	}
	detailsFile := fmt.Sprintf("%v.json", filepath.Join(detailsDir, infile))
	err = ioutil.WriteFile(detailsFile, []byte("["), 0644)
	if err != nil {
		return err
	}

	return nil
}

func closeDetailFile(outDir string, infilepath string) (err error) {
	infile := filepath.Base(infilepath)

	// close details file
	detailsDir := filepath.Join(outDir, "details")
	err = createOutDirIfNotExist(detailsDir)
	if err != nil {
		return err
	}
	detailsFile := fmt.Sprintf("%v.json", filepath.Join(detailsDir, infile))
	f, err := os.OpenFile(detailsFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString("\n]")
	if err != nil {
		return err
	}

	return nil
}

func writeOverallSummaryFile(outDir string, summaryErrStats map[string]ErrorStats) (err error) {
	if err := createOutDirIfNotExist(outDir); err != nil {
		return err
	}

	// save summary data
	summaryData, err := json.MarshalIndent(summaryErrStats, "", "  ")
	if err != nil {
		return err
	}
	summaryFile := filepath.Join(outDir, "summary.json")
	ioutil.WriteFile(summaryFile, summaryData, 0644)

	return nil
}

func writeValidationOutputs(outDir string, infilepath string, colrecs map[string][]records.RecordGetSetterWithError, errStats map[string]*ErrorStat, isFirst bool) (err error) {
	if err := createOutDirIfNotExist(outDir); err != nil {
		return err
	}

	infile := filepath.Base(infilepath)
	detailsDir := filepath.Join(outDir, "details")
	err = createOutDirIfNotExist(detailsDir)
	if err != nil {
		return err
	}

	recs := colrecs["default"]

	recWs := []RecordWrapper{}

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
					RecordCount:      0,
				}
				es.CalculatePercentage()
				errStats[errKey] = &es
				continue
			}

			errStats[errKey].IncErrCount()
		}

		recWs = append(recWs, RecordWrapper{
			Errors: errs,
			Record: o,
		})
	}

	// prepare details data to save and remove array wrappers
	detailsData, err := json.MarshalIndent(recWs, "", "  ")
	if err != nil {
		return err
	}
	detailsData = detailsData[1 : len(detailsData)-2]

	// write details data
	detailsFile := fmt.Sprintf("%v.json", filepath.Join(detailsDir, infile))
	f, err := os.OpenFile(detailsFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if !isFirst {
		// write a comma to join with previous record when not first record set
		_, err = f.WriteString(",")
		if err != nil {
			return err
		}
	}
	_, err = f.Write(detailsData)
	if err != nil {
		return err
	}

	return nil
}

func writeSummaryOutputs(outDir string, infilepath string, errStats map[string]*ErrorStat) (err error) {
	if err := createOutDirIfNotExist(outDir); err != nil {
		return err
	}

	infile := filepath.Base(infilepath)
	summaryDir := filepath.Join(outDir, "summary")
	err = createOutDirIfNotExist(summaryDir)
	if err != nil {
		return err
	}

	// save summary
	summaryData, err := json.MarshalIndent(errStats, "", "  ")
	if err != nil {
		return err
	}
	summaryFile := fmt.Sprintf("%v.json", filepath.Join(summaryDir, infile))
	err = ioutil.WriteFile(summaryFile, summaryData, 0644)
	if err != nil {
		return err
	}

	return nil
}

func readFile(filename string) (data []byte, err error) {
	data, err = ioutil.ReadFile(filename)
	if err != nil {
		fmt.Println("gotten error reading ", filename, ":", err.Error())
		return nil, err
	}

	return data, nil
}
