package qa

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	yaml "github.com/ghodss/yaml"

	"github.com/DataHenHQ/datahen/records"
	"github.com/DataHenHQ/henqa/customtypes"
	workflows "github.com/DataHenHQ/henqa_workflows"
	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/xeipuuv/gojsonschema"
)

type processFileStreamFn func(filename string, batchSize int, validateFn records.ValidateFn) error

type RecordWrapper struct {
	Errors []records.SchemaError `json:"errors"`
	Record interface{}           `json:"record"`
}

type RecordsValidationResult struct {
	Collections map[string][]RecordWrapper         `json:"collections"`
	Stats       map[string]*records.CollectionStat `json:"stats"`
}

func Validate(ins []string, schemas []string, wfname string, outDir string, summaryFile string, batchSize int, maxRecsWithErrors int) (err error) {
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

	err = validateWithSchema(files, mergedSchema, wfname, outDir, summaryFile, batchSize, maxRecsWithErrors)
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

func validateWithSchema(files []string, schema []byte, wfname string, outDir string, summaryFile string, batchSize int, maxRecsWithErrors int) (err error) {
	// load schema
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

	// load workflow
	wf, err := workflows.GetWorkflow(wfname)
	if err != nil {
		return err
	}

	// loop each file to validate
	summaryErrStats := map[string]customtypes.ErrorStats{}
	for _, f := range files {
		// validate file
		shouldContinue, err := validateSingleFile(f, colSchemaLoaders, wf, summaryErrStats, batchSize, outDir, maxRecsWithErrors)
		if err != nil {
			if shouldContinue {
				continue
			}
			return err
		}
	}

	// write overrall summary
	if err := writeOverallSummaryFile(outDir, summaryFile, summaryErrStats); err != nil {
		return err
	}

	return nil
}

func validateSingleFile(f string, colSchemaLoaders map[string]*gojsonschema.JSONLoader, wf *workflows.Workflow, summaryErrStats map[string]customtypes.ErrorStats, batchSize int, outDir string, maxRecsWithErrors int) (shouldContinue bool, err error) {
	// analyze file extension
	processFile, includeCollection, err := analyzeFileExtension(f)
	if err != nil {
		return true, err
	}

	// init detail file
	err = initDetailFile(outDir, f)
	if err != nil {
		fmt.Println("gotten error initializing output files for ", f, ":", err.Error())
		return false, err
	}

	// process the files and keep stats
	errStats := map[string]*customtypes.ErrorStat{}
	var recordCount uint64 = 0
	gvars := make(map[string]interface{})
	vbf := validateBatchFn(f, colSchemaLoaders, wf, gvars, outDir, includeCollection, &recordCount, errStats, maxRecsWithErrors)
	err = processFile(f, batchSize, vbf)
	if err != nil {
		fmt.Println("gotten error processing input file ", f, ":", err.Error())
		return false, err
	}

	// close detail file
	err = closeDetailFile(outDir, f)
	if err != nil {
		fmt.Println("gotten error initializing output files for ", f, ":", err.Error())
		return false, err
	}

	// execute workflow for summary
	if wf != nil {
		err := wf.ExecSummary(gvars, errStats)
		if err != nil {
			return false, err
		}
	}

	// fix errStats record counters and write summary
	for _, es := range errStats {
		es.RecordCount = recordCount
		es.CalculatePercentage()
	}
	writeSummaryOutputs(outDir, f, errStats)

	// map errors to summary stats file
	basefile := filepath.Base(f)
	summaryErrStats[basefile] = errStats
	fmt.Println("")
	return false, nil
}

func validateBatchFn(f string, colSchemaLoaders map[string]*gojsonschema.JSONLoader, wf *workflows.Workflow, gvars map[string]interface{}, outDir string, includeCollection bool, recordCount *uint64, errStats map[string]*customtypes.ErrorStat, maxRecsWithErrors int) records.ValidateFn {
	colstats := make(map[string]*records.CollectionStat)
	isFirst := true
	return func(recs []records.RecordGetSetterWithError) (err2 error) {
		collection := ""
		var totalRecWithErrors = 0

		// loop records and assign schema
		for _, rec := range recs {
			// skip when collection is empty
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

		// loop collection records and write validation outputs
		for _, recwes := range colrecs {
			// execute workflow for this record
			if wf != nil {
				for _, recwe := range recwes {
					err2 = wf.ExecRecord(recwe, gvars)
					if err2 != nil {
						return err2
					}
				}
			}

			// write validation outputs
			*recordCount += uint64(len(recwes))
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
	}
}

func analyzeFileExtension(f string) (processFile processFileStreamFn, includeCollection bool, err error) {
	switch filepath.Ext(f) {
	case ".csv":
		processFile = records.ProcessCSVFile
	case ".json":
		processFile = records.ProcessJSONFile
		includeCollection = true
	default:
		msg := fmt.Sprintf("%s is not a .csv or .json file. Skipping", f)
		fmt.Println(msg)
		return nil, false, errors.New(msg)
	}
	if err != nil {
		fmt.Println("gotten error reading ", f, ":", err.Error())
		return nil, false, err
	}
	fmt.Println("validating:", f)

	return processFile, includeCollection, nil
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

func writeOverallSummaryFile(outDir string, summaryFile string, summaryErrStats map[string]customtypes.ErrorStats) (err error) {
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

func writeValidationOutputs(outDir string, infilepath string, recwes []records.RecordGetSetterWithError, errStats map[string]*customtypes.ErrorStat, isFirst bool, includeCollection bool, totalRecWithErrors *int, maxRecsWithErrors int) (err error) {
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
				es := customtypes.ErrorStat{
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

func writeSummaryOutputs(outDir string, infilepath string, errStats map[string]*customtypes.ErrorStat) (err error) {
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
