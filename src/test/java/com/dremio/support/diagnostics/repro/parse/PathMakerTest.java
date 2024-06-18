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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class PathMakerTest {

  @Nested
  static class OnlySpace {
    private static DatasetPath listOfSpacesToMake;

    @BeforeAll
    static void initAll() {
      PathMaker pathMaker = new PathMaker();
      // don't get the last one as that is not a folder but a dataset
      listOfSpacesToMake = pathMaker.getListOfSpacesToMake("a");
    }

    @Test
    void testFindSpace() {
      assertThat(listOfSpacesToMake.getSpace()).isEqualTo("a");
    }

    @Test
    void testNoFoldersPresent() {
      assertThat(listOfSpacesToMake.getFolders().size()).isEqualTo(0);
    }
  }

  @Nested
  static class SpaceWithSource {
    private static DatasetPath listOfSpacesToMake;

    @BeforeAll
    static void initAll() {
      // we do not actually want to get the folder here..as this is just a space and a dataset
      PathMaker pathMaker = new PathMaker();
      // don't get the last one as that is not a folder but a dataset
      listOfSpacesToMake = pathMaker.getListOfSpacesToMake("a.b");
    }

    @Test
    void testHasSpace() {
      assertThat(listOfSpacesToMake.getSpace()).isEqualTo("a");
    }

    @Test
    void testNoFolderCreated() {
      assertThat(listOfSpacesToMake.getFolders().size()).isEqualTo(0);
    }
  }

  @Nested
  static class SpaceFoldersAndSource {
    private static DatasetPath listOfSpacesToMake;

    @BeforeAll
    static void initAll() {
      PathMaker pathMaker = new PathMaker();
      // don't get the last one as that is not a folder but a dataset
      listOfSpacesToMake = pathMaker.getListOfSpacesToMake("a.b.c.d");
    }

    @Test
    void testMakesSpace() {
      assertThat(listOfSpacesToMake.getSpace()).isEqualTo("a");
    }

    @Test
    void testMakes2Folders() {
      assertThat(listOfSpacesToMake.getFolders().size()).isEqualTo(2);
    }

    @Test
    void testFirstFolderHasSpaceAndOneFolderInPath() {
      assertThat(listOfSpacesToMake.getFolders().get(0)).isEqualTo(Arrays.asList("a", "b"));
    }

    @Test
    void testSecondFolderHasSpaceAndTwoFoldersInPath() {
      assertThat(listOfSpacesToMake.getFolders().get(1)).isEqualTo(Arrays.asList("a", "b", "c"));
    }
  }

  @Nested
  static class GetSpaceWithEscapedPeriods {
    private static DatasetPath listOfSpacesToMake;
    private static final String spaceWithDots = "a.a.a.a";

    @BeforeAll
    static void initAll() {
      PathMaker pathMaker = new PathMaker();
      listOfSpacesToMake = pathMaker.getListOfSpacesToMake("\"a.a.a.a\".b.c.d");
    }

    @Test
    void testSpaceIsFolderWithDots() {
      assertThat(listOfSpacesToMake.getSpace()).isEqualTo(spaceWithDots);
    }

    @Test
    void testThereAreTwoFolders() {
      assertThat(listOfSpacesToMake.getFolders().size()).isEqualTo(2);
    }

    @Test
    void testFirstFolderHasSpaceAndOneFolder() {
      assertThat(listOfSpacesToMake.getFolders().get(0))
          .isEqualTo(Arrays.asList(spaceWithDots, "b"));
    }

    @Test
    void testLastFolderHasSpaceAndTwoFolders() {
      assertThat(listOfSpacesToMake.getFolders().get(1))
          .isEqualTo(Arrays.asList(spaceWithDots, "b", "c"));
    }
  }

  @Nested
  static class GetFolderWithEscapedPeriods {
    private static DatasetPath listOfSpacesToMake;
    private static final String folderWithDots = "a.a.a.a";

    @BeforeAll
    static void initAll() {
      PathMaker pathMaker = new PathMaker();
      listOfSpacesToMake = pathMaker.getListOfSpacesToMake("a.\"a.a.a.a\".c.d");
    }

    @Test
    void testSpaceIsFolderWithDots() {
      assertThat(listOfSpacesToMake.getSpace()).isEqualTo("a");
    }

    @Test
    void testThereAreTwoFolders() {
      assertThat(listOfSpacesToMake.getFolders().size()).isEqualTo(2);
    }

    @Test
    void testFirstFolderHasSpaceAndOneFolder() {
      assertThat(listOfSpacesToMake.getFolders().get(0))
          .isEqualTo(Arrays.asList("a", folderWithDots));
    }

    @Test
    void testLastFolderHasSpaceAndTwoFolders() {
      assertThat(listOfSpacesToMake.getFolders().get(1))
          .isEqualTo(Arrays.asList("a", folderWithDots, "c"));
    }
  }
}
