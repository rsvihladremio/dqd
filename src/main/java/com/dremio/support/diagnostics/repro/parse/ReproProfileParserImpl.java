/**
 * Copyright 2022 Dremio
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.support.diagnostics.repro.parse;

import com.dremio.support.diagnostics.repro.*;
import com.dremio.support.diagnostics.shared.dto.profilejson.DatasetProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import com.google.common.base.Splitter;
import java.util.*;
import java.util.logging.Logger;

/**
 * the logic that ties in parsing of the profile json for the various pieces we need out of the
 * profile.json
 */
public class ReproProfileParserImpl implements ReproProfileParser {
  private static final Logger logger = Logger.getLogger(ReproProfileParserImpl.class.getName());
  private final PathMaker pathMaker = new PathMaker();
  private final VdsSorter vdsSorter;
  private final TableRefFinder tableRefFinder;
  private final SchemaDeserializer schemaDeserializer;
  private final ColumnDefYaml columnDef;

  /**
   * the coordination class that will convert a profile.json into a set of instructions for a
   * reproduction. This will either do the parsing itself or delegate off to other classes for the
   * more complex logic.
   *
   * @param columnDef override definitions for data. Use this when you want to match specific data
   *     conditions
   * @param vdsSorter the logic for sorting VDS order, useful for making sure VDSs are created in
   *     the correct order. Assumes the tableRefFinder knows what is is doing
   * @param tableRefFinder finds the table references in a VDS.
   * @param schemaDeserializer responsible for convert PDSs into Create table statements (CTAS for
   *     example)
   */
  public ReproProfileParserImpl(
      final ColumnDefYaml columnDef,
      final VdsSorter vdsSorter,
      final TableRefFinder tableRefFinder,
      final SchemaDeserializer schemaDeserializer) {
    this.columnDef = columnDef;
    this.vdsSorter = vdsSorter;
    this.tableRefFinder = tableRefFinder;
    this.schemaDeserializer = schemaDeserializer;
  }

  /**
   * For each VDS found, it creates a CREATE statement for the VDS based on it's dataset path for
   * the name and based on the sql that is stored in the profile
   *
   * @param profileJSON a parsed profile.json file
   * @return a list of VDSs sorted in order for creation
   */
  @Override
  public Collection<VdsSql> parseVDSs(ProfileJSON profileJSON) {
    Set<VdsSql> vds = new HashSet<>();
    if (profileJSON == null || profileJSON.getDatasetProfile() == null) {
      logger.warning("no profile.json to parse");
      return new ArrayList<>();
    }
    for (DatasetProfile dp : profileJSON.getDatasetProfile()) {
      if (dp.getType() == 2) {
        // type 2 is a VDS
        // make sure to remove silly windows new lines from sql
        String sql =
            String.format(
                "CREATE VDS %s as %s;", dp.getDatasetPath(), dp.getSql().replace("\r", ""));
        String[] tableRefs = this.tableRefFinder.searchSql(sql);
        logger.fine(
            String.format(
                "vds %s has table refs %s", dp.getDatasetPath(), String.join(",", tableRefs)));
        vds.add(new VdsSql(dp.getDatasetPath(), sql, tableRefs));
      }
    }

    List<VdsSql> vdsList = new ArrayList<>(vds);
    this.vdsSorter.sortVds(vdsList);
    return vdsList;
  }

  /**
   * Each found PDS will be handed off to the SchemaDeserializer which has the knowledge to retrieve
   * the PDS schema and convert it into SQL.
   *
   * @param profileJSON a parsed profile.json file
   * @return a list of PDS to create
   */
  @Override
  public Collection<PdsSql> parsePDSs(ProfileJSON profileJSON) {
    List<PdsSql> pds = new ArrayList<>();
    final List<String> pdsTableNames = new ArrayList<>();
    for (DatasetProfile dp : profileJSON.getDatasetProfile()) {
      if (dp.getType() == 1) {
        // type 1 is a PDS
        // we have to make sure we haven't already added one
        if (pdsTableNames.contains(dp.getDatasetPath())) {
          // skip our duplicates
          continue;
        }
        // now go ahead and add that tablename to the list so we don't duplicate it
        pdsTableNames.add(dp.getDatasetPath());
        // generate a pds depending on the deserializer passed in, it can use anything from the
        // datasetProfile
        String schema = this.schemaDeserializer.readSchema(dp);
        pds.add(new PdsSql(dp.getDatasetPath(), schema));
      }
    }
    // validate that we do not run into an issue with overrides
    this.columnDef
        .getTables()
        .forEach(
            x -> {
              if (!pdsTableNames.contains(x.getName())) {
                throw new InvalidOverrideException(x.getName(), pdsTableNames);
              }
            });
    return pds;
  }

  /**
   * This will parse all VDSs for a list of spaces. This is a tricky bit of code and so delegates
   * most of the work to the PathMaker#getListOfSpacesToMake method.
   *
   * @param profileJSON is a parsed profile.json file
   * @return list of spaces to create
   */
  @Override
  public Collection<String> parseSpaces(ProfileJSON profileJSON) {
    Set<String> spaces = new HashSet<>();
    for (DatasetProfile dp : profileJSON.getDatasetProfile()) {
      if (dp.getType() == 2) {
        // type 2 is a VDS
        spaces.add(dp.getDatasetPath());
      }
    }
    Set<String> list = new HashSet<>();
    for (String space : spaces) {
      DatasetPath path = pathMaker.getListOfSpacesToMake(space);
      list.add(path.getSpace());
    }
    return new ArrayList<>(list);
  }

  /**
   * @param profileJSON is a parsed profile.json file
   * @return nested list of folders
   */
  @Override
  public Collection<Collection<String>> parseFolders(ProfileJSON profileJSON) {
    Set<String> spaces = new HashSet<>();
    for (DatasetProfile dp : profileJSON.getDatasetProfile()) {
      if (dp.getType() == 2) {
        // type 2 is a VDS
        spaces.add(dp.getDatasetPath());
      }
    }
    Set<List<String>> hash = new HashSet<>();
    for (String space : spaces) {
      DatasetPath paths = pathMaker.getListOfSpacesToMake(space);
      for (List<String> folder : paths.getFolders()) {
        if (paths.getFolders().size() > 0) {
          hash.add(folder);
        }
      }
    }
    List<Collection<String>> list = new ArrayList<>(hash);
    // sort so it is in order
    list.sort(Comparator.comparing(x -> String.join(".", x)));
    return list;
  }

  /**
   * Will get all the text between periods and assume this represents a space to create
   *
   * @param profileJSON is a parsed profile.json file
   * @return list of sources from the dataset paths
   */
  @Override
  public Collection<String> parseSources(ProfileJSON profileJSON) {
    Set<String> sources = new HashSet<>();
    for (DatasetProfile dp : profileJSON.getDatasetProfile()) {
      // type 1 is a PDS
      if (dp.getType() == 1) {
        // be careful here, if there are ways to escape periods this could lead to bugs
        final List<String> tokens = Splitter.on('.').splitToList(dp.getDatasetPath());
        if (tokens.size() > 0) {
          sources.add(tokens.get(0));
        } else {
          logger.warning("empty source found, this is likely a bug");
        }
      }
    }
    return new ArrayList<>(sources);
  }
}
