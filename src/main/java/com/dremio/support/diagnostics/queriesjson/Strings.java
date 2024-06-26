package com.dremio.support.diagnostics.queriesjson;

public class Strings {
  private static final String R = "_";

  /**
   * used for escaping for javascript variable names
   *
   * @param name variable name to escape for special characeters, it's already named to -
   */
  public static String escape(String name) {
    return name.replace(" ", R)
        .replace("[", R)
        .replace("]", R)
        .replace("|", R)
        .replace("%", R)
        .replace("^", R)
        .replace("=", R)
        .replace("+", R)
        .replace("#", R)
        .replace("&", R)
        .replace("@", R)
        .replace("*", R)
        .replace("$", R)
        .replace("\'", R)
        .replace("\"", R)
        .replace("\\", R)
        .replace("/", R)
        .replace(">", R)
        .replace("<", R)
        .replace("!", R)
        .replace("?", R)
        .replace(",", R)
        .replace(".", R)
        .replace(":", R)
        .replace(";", R)
        .replace("-", R)
        .replace("(", R)
        .replace(")", R)
        .replace("]", R)
        .replace("[", R);
  }
}
