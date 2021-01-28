package qa

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	yaml "github.com/ghodss/yaml"

	"github.com/DataHenHQ/datahen/records"
	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/xeipuuv/gojsonschema"
)

type processFileStreamFn func(filename string, batchSize int, validateFn records.ValidateFn) error

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

func Validate(ins []string, schemas []string, outDir string, summaryFile string, batchSize int, maxRecsWithErrors int) (err error) {
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

	err = validateWithSchema(files, mergedSchema, outDir, summaryFile, batchSize, maxRecsWithErrors)
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

		// convert yaml to json if not already
		switch filepath.Ext(f) {
		case ".yaml", ".yml":
			nj, err := yaml.YAMLToJSON(nschema)
			if err != nil {
				fmt.Printf("error converting YAML to JSON: %v\n", err)
				continue
			}
			nschema = nj
		}

		// if the first schema, then simply assign it
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

func validateWithSchema(files []string, schema []byte, outDir string, summaryFile string, batchSize int, maxRecsWithErrors int) (err error) {
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
	for _, f := range files {
		var processFile processFileStreamFn = nil
		includeCollection := false

		switch filepath.Ext(f) {
		case ".csv":
			processFile = records.ProcessCSVFile
		case ".json":
			processFile = records.ProcessJSONFile
			includeCollection = true
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
		err = processFile(f, batchSize, func(recs []records.RecordGetSetterWithError) (err2 error) {
			collection := ""
			var totalRecWithErrors = 0

			for _, rec := range recs {
				collection = rec.GetCollection()
				if collection == "" {
					continue
				}

				// ensure schemal loader exits for all collections
				if _, ok := colSchemaLoaders[collection]; !ok {
					colSchemaLoaders[collection] = colSchemaLoaders["default"]
				}
			}

			// validate collection records
			colrecs := records.SchemaLoadersValidate(colSchemaLoaders, recs, colstats)
			for _, recwes := range colrecs {
				recordCount += uint64(len(recwes))
				err2 = writeValidationOutputs(outDir, f, recwes, errStats, isFirst, includeCollection, &totalRecWithErrors, maxRecsWithErrors)
				if err2 != nil {
					return err2
				}
				if len(errStats) > 0 {
					isFirst = false
				}
			}
			fmt.Print(".")

			return nil
		})
		if err != nil {
			fmt.Println("gotten error processing input file ", f, ":", err.Error())
			return err
		}

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
		fmt.Println("")
	}

	if err := writeOverallSummaryFile(outDir, summaryFile, summaryErrStats); err != nil {
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

func writeOverallSummaryFile(outDir string, summaryFile string, summaryErrStats map[string]ErrorStats) (err error) {
	if err := createOutDirIfNotExist(outDir); err != nil {
		return err
	}

	// save summary data
	summaryData, err := json.MarshalIndent(summaryErrStats, "", "  ")
	if err != nil {
		return err
	}
	summaryFileName := filepath.Join(outDir, fmt.Sprintf("%v.json", summaryFile))
	ioutil.WriteFile(summaryFileName, summaryData, 0644)

	return nil
}

func writeValidationOutputs(outDir string, infilepath string, recwes []records.RecordGetSetterWithError, errStats map[string]*ErrorStat, isFirst bool, includeCollection bool, totalRecWithErrors *int, maxRecsWithErrors int) (err error) {
	if err := createOutDirIfNotExist(outDir); err != nil {
		return err
	}

	infile := filepath.Base(infilepath)
	detailsDir := filepath.Join(outDir, "details")
	err = createOutDirIfNotExist(detailsDir)
	if err != nil {
		return err
	}

	recWs := []RecordWrapper{}

	for _, rec := range recwes {
		o := records.TransformToRecordJSONB(rec)

		// delete any unused field from datahen's output records
		if !includeCollection {
			delete(o, "_collection")
		}

		errs := rec.GetErrors()
		if errs == nil {
			continue
		}

		// Increment the overal total errors
		*totalRecWithErrors++

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

		// if max records with errors is specified, then limit the output
		if maxRecsWithErrors == -1 || (maxRecsWithErrors > -1 && *totalRecWithErrors <= maxRecsWithErrors) {
			recWs = append(recWs, RecordWrapper{
				Errors: errs,
				Record: o,
			})
		}

	}

	// prepare details data to save and remove array wrappers
	detailsData, err := json.MarshalIndent(recWs, "", "  ")
	if err != nil {
		return err
	}
	upperLimit := 1
	if len(detailsData) > 2 {
		upperLimit = len(detailsData) - 2
	}
	detailsData = detailsData[1:upperLimit]

	// write details data
	detailsFile := fmt.Sprintf("%v.json", filepath.Join(detailsDir, infile))
	f, err := os.OpenFile(detailsFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if !isFirst && len(recWs) > 0 {
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
