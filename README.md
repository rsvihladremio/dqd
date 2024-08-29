# dqd: Dremio Query Doctor!

Diagnosis all the reasons why things are broken.

NOTE: THIS IS NOT ASSOCIATED WITH DREMIO AND IS NOT AN OFFICIAL SOFTWARE PRODUCT OF DREMIO

### Get started On Mac/Linux using homebrew

```bash
brew tap rsvihladremio/tools
brew install dqd
dqd server
```
navigate to http://localhost:8080

## CLI usage 

This provides an alternative to the web ui and some people prefer it as a general workflow. The same reports are generated in the CLI as in the web server.

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

## Goals

* One-stop shop for all tools
* Solve 20% of cases 100x faster, i.e. algorithmic and simple, but often a needle in a haystack in the profile viewers
* Make 60% of cases faster to solve by some measure. Solve the first pass analysis issue, move onto more in depth interesting problems
* Send no false recommendations or diagnostic signals, err on the side of caution

### Non Goals

* To solve all tickets instantly, not realistic
