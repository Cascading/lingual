/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.lingual.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Version
  {
  private static final Logger LOG = LoggerFactory.getLogger( Version.class );

  public static final String LINGUAL_RELEASE_MAJOR = "lingual.release.major";
  public static final String LINGUAL_RELEASE_MINOR = "lingual.release.minor";
  public static final String LINGUAL_BUILD_NUMBER = "lingual.build.number";
  public static final String LINGUAL = "Lingual";


  public static Properties versionProperties;

  public static String getName()
    {
    return LINGUAL;
    }

  public static String getVersionString()
    {
    return getFullVersionString();
    }

  public static String getProductName()
    {
    return "Cascading";
    }

  public static String getProductVersion()
    {
    return createReleaseVersion( cascading.util.Version.getReleaseFull(), cascading.util.Version.getReleaseBuild() );
    }

  public static int getMajorVersion()
    {
    String version = getReleaseMajor();

    if( version == null )
      return 1;

    return Integer.parseInt( version.split( "\\." )[ 0 ] );
    }

  public static int getMinorVersion()
    {
    String version = getReleaseMajor();

    if( version == null )
      return 0;

    return Integer.parseInt( version.split( "\\." )[ 1 ] );
    }

  public static String getBannerVersionString()
    {
    return String.format( "Concurrent, Inc - %s %s", LINGUAL, getFullVersionString() );
    }

  public static String getFullVersionString()
    {
    if( getVersionProperties().isEmpty() )
      return "[dev]";

    return createReleaseVersion( getReleaseFull(), getReleaseBuild() );
    }

  private static String createReleaseVersion( String releaseFull, String releaseBuild )
    {
    String releaseVersion;

    if( releaseBuild == null || releaseBuild.isEmpty() )
      releaseVersion = String.format( "%s", releaseFull );
    else
      releaseVersion = String.format( "%s-%s", releaseFull, releaseBuild );

    return releaseVersion;
    }

  public static String getReleaseFull()
    {
    String releaseFull;

    if( getReleaseMinor() == null || getReleaseMinor().isEmpty() )
      releaseFull = getReleaseMajor();
    else
      releaseFull = String.format( "%s.%s", getReleaseMajor(), getReleaseMinor() );

    return releaseFull;
    }

  public static boolean hasMajorMinorVersionInfo()
    {
    return !cascading.util.Util.isEmpty( getReleaseMinor() ) && !cascading.util.Util.isEmpty( getReleaseMajor() );
    }

  public static boolean hasAllVersionInfo()
    {
    return !cascading.util.Util.isEmpty( getReleaseBuild() ) && hasMajorMinorVersionInfo();
    }

  public static String getReleaseBuild()
    {
    return getVersionProperties().getProperty( LINGUAL_BUILD_NUMBER );
    }

  public static String getReleaseMinor()
    {
    return getVersionProperties().getProperty( LINGUAL_RELEASE_MINOR );
    }

  public static String getReleaseMajor()
    {
    return getVersionProperties().getProperty( LINGUAL_RELEASE_MAJOR );
    }

  private static synchronized Properties getVersionProperties()
    {
    try
      {
      if( versionProperties == null )
        {
        versionProperties = loadVersionProperties();

        if( versionProperties.isEmpty() )
          LOG.warn( "unable to load version information" );
        }
      }
    catch( IOException exception )
      {
      LOG.warn( "unable to load version information", exception );
      versionProperties = new Properties();
      }

    return versionProperties;
    }

  public static Properties loadVersionProperties() throws IOException
    {
    Properties properties = new Properties();

    InputStream stream = Version.class.getClassLoader().getResourceAsStream( "cascading/lingual/version.properties" );

    if( stream == null )
      return properties;

    properties.load( stream );

    stream = Version.class.getClassLoader().getResourceAsStream( "cascading/lingual/build.number.properties" );

    if( stream != null )
      properties.load( stream );

    return properties;
    }
  }
