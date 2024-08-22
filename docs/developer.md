# developer documentation

## Rationale

Dremio Query Doctor was originally created as a CLI tool to give quick automated analysis of Dremio query profiles because the existing tools included with Dremio required a lot of manual copy and pasting to get any analysis done.
So DQD was born.

### Overall Structure

Being an application that was developed to help solve tickets in process, the software is very light on structure. This can make navigating the code a challenge but I will attempt to cover the overall design here. There is a CLI that has a number of subcommands which drive various components:

```bash
Commands:
  help                    Display help information about the specified command.
  profile-json            analyze a profile json and make recommendations
  summarize-profile-json  A simplified profile-json command that only includes
                            a brief summary
  queries-json            analyze a queries.json file and make recommendations
  repro                   given a profile.json or zip containing one, this
                            command will generate a file with schemas for all
                            of the pds and vds found in the profile. It will
                            first attempt to use the arrow schema and then
                            failing that fall back to using a guess based on
                            the query
  server                  run a web ui that is similar to the CLI but more rich
                            with interactive graphs, starts on port 8080
```

these are all largely split up into their own modules with some code sharing in between (archive parsing, the ability to read profile.json files)

profile-json - largely is under `src/main/java/com/dremio/support/diagnostics/profilejson`
summarize-profile-json - largely is under `src/main/java/com/dremio/support/diagnostics/profilejson/singlefile`
queries-json - is largerly under `src/main/java/com/dremio/support/diagnostics/queriesjson`
repro - is largely under `src/main/java/com/dremio/support/diagnostics/repro`
server - is largely under `src/main/java/com/dremio/support/diagnostics/profilejson`

### profile-json

The overall flow is broadly simple (but there is an awful lot of work in step 2), this actually gets surprisingly complex and it required a lot of reading dremio-oss code to figure out how the existing tools calculated things in sometimes non obvious ways. 

1. Reads a profile.json file (or a zip containing one) parse all the necessary bits out of it.
2. Summarizes and aggregates a lot of the detail from the operators, phases and node information.
3. Write it out to an html file. 

### summarize-profile-json

This is the same as profile-json except it aims primarily just to list operators and make them searchable and exportable. This was an alternative perspective provided so that we could have a simpler to understand user interface

It does broadly the following: 

1. Reads a profile.json file (or a zip containing one) parse all the necessary bits out of it.
2. Summarizes and aggregates a lot of the detail from the operators
3. Write it out to an html file. 

### queries-json

This will read a an archive of queries.json files (either standalone or inside of a ddc tarball) and generate an html
file that reports on the contents. This is the most popular part of DQD and the main thing in use inside of Dremio support.

It does broadly the following:

1. Scans an archive for files that match
2. for each file found:
    1. spawns a thread or blocks if the thread pool is out of slots
    2. extracts that file to disk
    3. reads each row 
    4. runs each row through a series of thread safe "reporters" that aggregate on that row
    5. deletes the extracted file
3. finally when all files are read an html file is generated from the "reporters" and the process is complete

### repro 

This will parse the datasets used out of profile.json and attempt to generate a series of ctas statements either via direct api calls or via writing a bash script which uses a series of curl statements. This has largely been replaced by other tools internally but has been left in dqd for historical reasons.

It does broadly the following:

1. parses the datasets
2. walking the number of "." it determines how many spaces and folders we need to create
3. is also will do a best effort at parsing the query to determine what dependencies the query has and order then correctly
4. output
    1. then if the script generation is used, a bash script will be created from this information
    2. if direct java connection is used then a dremio rest api will be used to generate spaces, folders, pds, then views

### server

This is a web ui that makes several of the CLI tools more accessible, this is largely driven by the need to have a single endpoint that people can rely on. There are performance downsides as for example on large 60gb tarballs that are sometimes generated the resulting output can take a very long time to upload.

Broadly this is just a web server running Javalin https://javalin.io/ with https://www.beercss.com/ providing the user interface. When uploads are done then the file is handed off effectively to the same entry points as the above CLI tools and the resulting html is output to user. 

The site has no permalinking and it's basically an online CLI, this was to save on complexity and infrastructure needs
