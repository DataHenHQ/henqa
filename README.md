# DataHen QA Tool

To use:

```
$ henqa inputfile.json or/the/inputdir --schema person.json 
```

To see available commands:
```
$ henqa help
```

## To use Github Action to build and push a new release
To do a release, you must tag your commit with semantic versioning tags like `v.0.1.2`, otherwise it will not run the builder.

```bash
# on the project directory
$ git tag <your tag> # for example v.1.2.3
$ git push origin <your tag>  # or example v.1.2.3
```
