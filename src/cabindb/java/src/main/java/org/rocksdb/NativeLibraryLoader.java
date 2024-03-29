// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
package org.cabindb;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.cabindb.util.Environment;

/**
 * This class is used to load the CabinDB shared library from within the jar.
 * The shared library is extracted to a temp folder and loaded from there.
 */
public class NativeLibraryLoader {
  //singleton
  private static final NativeLibraryLoader instance = new NativeLibraryLoader();
  private static boolean initialized = false;

  private static final String sharedLibraryName = Environment.getSharedLibraryName("cabindb");
  private static final String jniLibraryName = Environment.getJniLibraryName("cabindb");
  private static final String jniLibraryFileName = Environment.getJniLibraryFileName("cabindb");
  private static final String tempFilePrefix = "libcabindbjni";
  private static final String tempFileSuffix = Environment.getJniLibraryExtension();

  /**
   * Get a reference to the NativeLibraryLoader
   *
   * @return The NativeLibraryLoader
   */
  public static NativeLibraryLoader getInstance() {
    return instance;
  }

  /**
   * Firstly attempts to load the library from <i>java.library.path</i>,
   * if that fails then it falls back to extracting
   * the library from the classpath
   * {@link org.cabindb.NativeLibraryLoader#loadLibraryFromJar(java.lang.String)}
   *
   * @param tmpDir A temporary directory to use
   *   to copy the native library to when loading from the classpath.
   *   If null, or the empty string, we rely on Java's
   *   {@link java.io.File#createTempFile(String, String)}
   *   function to provide a temporary location.
   *   The temporary file will be registered for deletion
   *   on exit.
   *
   * @throws java.io.IOException if a filesystem operation fails.
   */
  public synchronized void loadLibrary(final String tmpDir) throws IOException {
    try {
        System.loadLibrary(sharedLibraryName);
    } catch(final UnsatisfiedLinkError ule1) {
      try {
        System.loadLibrary(jniLibraryName);
      } catch(final UnsatisfiedLinkError ule2) {
        loadLibraryFromJar(tmpDir);
      }
    }
  }

  /**
   * Attempts to extract the native CabinDB library
   * from the classpath and load it
   *
   * @param tmpDir A temporary directory to use
   *   to copy the native library to. If null,
   *   or the empty string, we rely on Java's
   *   {@link java.io.File#createTempFile(String, String)}
   *   function to provide a temporary location.
   *   The temporary file will be registered for deletion
   *   on exit.
   *
   * @throws java.io.IOException if a filesystem operation fails.
   */
  void loadLibraryFromJar(final String tmpDir)
      throws IOException {
    if (!initialized) {
      System.load(loadLibraryFromJarToTemp(tmpDir).getAbsolutePath());
      initialized = true;
    }
  }

  File loadLibraryFromJarToTemp(final String tmpDir)
          throws IOException {
    final File temp;
    if (tmpDir == null || tmpDir.isEmpty()) {
      temp = File.createTempFile(tempFilePrefix, tempFileSuffix);
    } else {
      final File parentDir = new File(tmpDir);
      if (!parentDir.exists()) {
        throw new RuntimeException(
            "Directory: " + parentDir.getAbsolutePath() + " does not exist!");
      }
      temp = new File(parentDir, jniLibraryFileName);
      if (temp.exists() && !temp.delete()) {
        throw new RuntimeException("File: " + temp.getAbsolutePath()
            + " already exists and cannot be removed.");
      }
      if (!temp.createNewFile()) {
        throw new RuntimeException("File: " + temp.getAbsolutePath()
            + " could not be created.");
      }
    }

    if (!temp.exists()) {
      throw new RuntimeException("File " + temp.getAbsolutePath() + " does not exist.");
    } else {
      temp.deleteOnExit();
    }

    // attempt to copy the library from the Jar file to the temp destination
    try (final InputStream is = getClass().getClassLoader().
      getResourceAsStream(jniLibraryFileName)) {
      if (is == null) {
        throw new RuntimeException(jniLibraryFileName + " was not found inside JAR.");
      } else {
        Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
    }

    return temp;
  }

  /**
   * Private constructor to disallow instantiation
   */
  private NativeLibraryLoader() {
  }
}
