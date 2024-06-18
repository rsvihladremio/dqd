# dqd: Dremio Query Doctor!

Diagnosis all the reasons why things are broken.

NOTE: THIS IS NOT ASSOCIARED WITH DREMIO AND IS NOT AN OFFICIAL SOFTWARE PRODUCT OF DREMIO

## Get started

1. download file from releases
2. unzip and copy to a location in your path (or just remember where you want to run it from)
3. run one of the following commands depending on your use case

### Run the web UI

Similar to the CLI but more rich with interactive graphs, starts on port 8080

	dqd server

navigate to http://localhost:8080

### Analyze profile.json

Analyze a profile json and make recommendations

	dqd profile-json profile.json.zip

### Generate profile.json html summary page

After running this command open the index.html file in any modern browser

	dqd simplified-profile-json profile.json > index.html

### Compare two profiles

Different report from the default analysis, focuses on the comparison between the two profiles and does diffs on the plan

	dqd profile-json 1st.zip -c 2nd.zip --show-plan-details

### Run a reproduction against Dremio

Tiven a profile.json or zip containing one, this command will generate a file with schemas for all of the pds and vds found in the profile. It will first attempt to use the arrow schema and then failing that fall back to using a guess based on the query parameters. This is at best beta and may require significant manual creation of datasets and vds

	dqd repro --host http://localhost:9047 -u user -p pass profile.json.zip

### Analyze queries.json

Analyzes a tarball of queries.json file and make recommendations.

	dqd queries-json queries.json.gz

## Errata

* Hosted at https://dqd.drem.io
* How to install [here](#how-to-install)
* How to use [here](#how-to-use)

## Goals

* One-stop shop for all tools
* Solve 20% of cases 100x faster, i.e. algorithmic and simple, but often a needle in a haystack in the profile viewers
* Make 60% of cases faster to solve by some measure. Solve the first pass analysis issue, move onto more in depth interesting problems
* Send no false recommendations or diagnostic signals, err on the side of caution

### Non Goals

* To solve all tickets instantly, not realistic
