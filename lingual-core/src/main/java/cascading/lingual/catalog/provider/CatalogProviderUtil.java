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

package cascading.lingual.catalog.provider;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.WeakReference;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.Properties;
import java.util.WeakHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;

import cascading.lingual.jdbc.Driver;
import cascading.lingual.platform.PlatformBroker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogProviderUtil
  {
  private static final Logger LOG = LoggerFactory.getLogger( CatalogProviderUtil.class );

  protected static Map<String, WeakReference<CatalogProviderUtil>> instances =
    new WeakHashMap<String, WeakReference<CatalogProviderUtil>>(); // by localRepositoryPath

  protected File localRepository;

  /**
   * @param bootstrapProperties expected to contain CATALOG_ROOT_PATH_PROP, META_DATA_DIR_NAME_PROP, LOCAL_REPO_DIR_NAME_PROP properties
   *                            to resolve localRepository path
   * @return instance
   */
  public static CatalogProviderUtil getInstance( Properties bootstrapProperties )
    {
    return getInstance( bootstrapProperties, CatalogProviderUtil.class, instances );
    }

  protected static <T extends CatalogProviderUtil> T getInstance( Properties bootstrapProperties, Class<T> clazz, Map<String, WeakReference<T>> map )
    {
    String localRepositoryPath = buildLocalRepositoryPath( bootstrapProperties );

    WeakReference<T> ref = map.get( localRepositoryPath );
    T instance = ( ref == null ) ? null : ref.get();

    if( instance == null )
      {
      synchronized( map )
        {
        ref = map.get( localRepositoryPath );

        if( ref != null )
          instance = ref.get();

        if( instance == null )
          {
          try
            {
            instance = clazz.newInstance();
            instance.localRepository = new File( new File( localRepositoryPath ).toURI().normalize().getPath() );
            instance.localRepository.mkdirs();
            LOG.debug( "using local repository at {}", instance.localRepository.getAbsolutePath() );
            }
          catch( Exception e )
            {
            LOG.error( "unexpected failure instantiating {}, with exception {}", clazz.getName(), e );
            }
          ref = new WeakReference<T>( instance );
          instance = ref.get();
          map.put( localRepositoryPath, ref );
          }
        }
      }

    return instance;
    }

  public File getLocalRepository()
    {
    return localRepository;
    }

  public static String buildLocalRepositoryPath( Properties bootstrapProperties )
    {
    if( bootstrapProperties == null )
      bootstrapProperties = new Properties();
    String localRepositoryPath = bootstrapProperties.getProperty( PlatformBroker.LOCAL_REPO_FULL_PATH_PROP );
    if( localRepositoryPath == null )
      {
      StringBuilder sb = new StringBuilder();
      sb.append( bootstrapProperties.getProperty( Driver.CATALOG_PROP, "." ) )
        .append( '/' ).append( bootstrapProperties.getProperty( PlatformBroker.META_DATA_DIR_NAME_PROP, PlatformBroker.META_DATA_DIR_NAME ) )
        .append( '/' ).append( bootstrapProperties.getProperty( PlatformBroker.LOCAL_REPO_DIR_NAME_PROP, PlatformBroker.LOCAL_REPO_DIR_NAME ) );
      localRepositoryPath = sb.toString();
      }
    return localRepositoryPath;
    }

  /**
   * Copy jar file only (no *.pom, no dependencies) from url location to local repository
   *
   * @param urlPathStartsAfter   pick url path following this value, if not null and found in the url
   *                             e.g. "filepath=" for "http://search.maven.org/remotecontent?filepath=log4j/log4j/1.2.17/log4j-1.2.17.jar"
   * @param urlPathPrefixToStrip prefix of url path part to strip, or null,
   *                             e.g. "/repo/cascading" for "http://conjars.org/repo/cascading/avro/avro-scheme/2.1.1/avro-scheme-2.1.1.jar";
   *                             special value "*" strips all path elements except last (usually file name but not always, e.g. urls with query string)
   * @return jar file in localRepository or null on error
   */
  public File loadArtifactFromUrl( String urlString, String urlPathStartsAfter, String urlPathPrefixToStrip, boolean overwrite )
    {
    InputStream inputStream = null;
    OutputStream outputStream = null;

    // normalize path ('///', '/./', etc.)
    int index = urlString.indexOf( "://" );

    if( index < 0 )
      {
      urlString = "file://" + urlString;
      index = urlString.indexOf( "://" );
      }

    index += "://".length();

    String sourcePath = urlString.substring( index );

    if( urlString.startsWith( "file:" ) )
      sourcePath = new File( sourcePath ).toURI().normalize().getPath();

    urlString = urlString.substring( 0, index ) + sourcePath;

    try
      {
      // build path to the jar in localRepository
      URL url = new URL( urlString );
      String jarPath = url.getPath();
      if( urlPathStartsAfter != null && ( index = urlString.indexOf( urlPathStartsAfter ) ) >= 0 )
        jarPath = urlString.substring( index + urlPathStartsAfter.length() );
      if( "*".equals( urlPathPrefixToStrip ) ) // strip all path except file name
        jarPath = jarPath.substring( new File( jarPath ).getParent().length() );
      else if( urlPathPrefixToStrip != null && jarPath.startsWith( urlPathPrefixToStrip ) )
        jarPath = jarPath.substring( urlPathPrefixToStrip.length() );

      if( !jarPath.startsWith( "/" ) )
        jarPath = '/' + jarPath;

      if( !jarPath.startsWith( localRepository.getAbsolutePath() ) )
        jarPath = localRepository.getAbsolutePath() + jarPath;

      File outFile = new File( jarPath );

      // check if need to copy
      if( outFile.toURI().toURL().equals( url ) )
        {
        LOG.debug( "skipping copy to self: {}", url );
        return outFile;
        }

      if( !overwrite && outFile.exists() )
        {
        LOG.debug( "file exists: {}", outFile );
        return outFile;
        }

      // copy
      inputStream = url.openStream();

      BufferedInputStream bis = new BufferedInputStream( inputStream );

      if( outFile.getParent() != null )
        new File( outFile.getParent() ).mkdirs();

      outFile.createNewFile();
      outputStream = new FileOutputStream( outFile );

      BufferedOutputStream bos = new BufferedOutputStream( outputStream );
      byte buffer[] = new byte[ 4096 ];

      while( true )
        {
        int nRead = bis.read( buffer, 0, buffer.length );

        if( nRead <= 0 )
          break;

        bos.write( buffer, 0, nRead );
        }

      bos.flush();

      return outFile;
      }
    catch( Exception exception )
      {
      LOG.error( "cannot load {}, with exception: {}", urlString, exception );
      return null;
      }
    finally
      {
      try
        {
        if( outputStream != null )
          outputStream.close();

        if( inputStream != null )
          inputStream.close();
        }
      catch( Exception exception )
        {
        // do nothing
        }
      }
    }

  public static Class loadClass( File jarFile, ClassLoader parentClassLoader, String className )
    {
    if( parentClassLoader == null )
      parentClassLoader = Thread.currentThread().getContextClassLoader();

    try
      {
      URL[] urls = new URL[ 1 ];
      urls[ 0 ] = jarFile.toURI().toURL();

      URLClassLoader classLoader = URLClassLoader.newInstance( urls, parentClassLoader );

      return classLoader.loadClass( className );
      }
    catch( Exception exception )
      {
      LOG.error( "class {} loading from {} failed", className, jarFile.getName() );
      LOG.error( "with exception {}", exception );
      return null;
      }
    }

  /**
   * Convert jar+resource to a URL like this:
   * jar:file:/tmp/temp-repo/log4j/log4j/1.2.17/log4j-1.2.17.jar!/org/apache/log4j/lf5/config/defaultconfig.properties
   * No actual checks are made.
   */
  public static URL getResourceUrl( File jarFile, String resourceName )
    {
    try
      {
      String url = jarFile.toURI().toURL().toString();
      return new URL( "jar:" + url + "!/" + resourceName );
      }
    catch( Exception exception )
      {
      LOG.debug( "cannot get resource URL: {} in {}", resourceName, jarFile );
      LOG.debug( "with exception: {}", exception );
      return null;
      }
    }

  /**
   * Open InputStream for a Jar resource.
   * Caller must close.
   *
   * @return input stream or null if resource not found
   */
  public static InputStream getResourceAsStream( File jarFile, String resourceName )
    {
    try
      {
      @SuppressWarnings("resource") // close InputStream outside
        JarFile jarFile2 = new JarFile( jarFile, false );
      JarEntry jarEntry = jarFile2.getJarEntry( resourceName );

      if( jarEntry == null )
        throw new RuntimeException( "jar entry not found" );

      return jarFile2.getInputStream( jarEntry );
      }
    catch( Exception exception )
      {
      LOG.debug( "cannot get resource {} from {}", resourceName, jarFile );
      LOG.debug( "with exception: {}", exception );
      return null;
      }
    }

  /**
   * Load *.properties resource into java.util.Properties
   *
   * @return properties object or null if resource not found
   */
  public static Properties getPropertiesResource( File jarFile, String resourceName )
    {
    InputStream is = getResourceAsStream( jarFile, resourceName );

    return getPropertiesFromStream( is );
    }

  /**
   * Load *.properties resource into java.util.Properties
   *
   * @param inStream     the in stream
   * @param resourceName the resource name
   * @return properties object or null if resource not found
   * @throws InvalidProviderException
   */
  public static Properties getPropertiesResource( JarInputStream inStream, String resourceName )
    {
    try
      {
      // position the input stream at the given resource.
      JarEntry entry = inStream.getNextJarEntry();

      while( ( entry != null ) && ( !resourceName.equals( entry.getName() ) ) )
        entry = inStream.getNextJarEntry();

      if( entry == null )
        throw new InvalidProviderException( "unable to find resource " + resourceName + " in provider jar" );

      Properties properties = new Properties();

      // calling load on JarInputStream get only the current resource.
      properties.load( inStream );

      return properties;
      }
    catch( IOException ioe )
      {
      throw new InvalidProviderException( "unable to read resource " + resourceName + " in provider jar", ioe );
      }
    }

  /**
   * Load *.properties resource into java.util.Properties
   *
   * @return properties object or null if resource not found
   */
  public static Properties getPropertiesFromStream( InputStream inStream )
    {
    if( inStream == null )
      return null;

    Properties properties = new Properties();

    try
      {
      properties.load( inStream );
      }
    catch( Exception exception )
      {
      throw new RuntimeException( "unable to load properties", exception );
      }
    finally
      {
      try
        {
        inStream.close();
        }
      catch( IOException exception )
        {
        // do nothing
        }
      }

    return properties;
    }

  public static Properties getProviderProperties( File jarFile, boolean validate )
    {
    try
      {
      JarInputStream jarInputStream = new JarInputStream( new BufferedInputStream( new FileInputStream( jarFile ) ) );

      return getProviderProperties( jarInputStream, validate );
      }
    catch( IOException exception )
      {
      throw new InvalidProviderException( "error reading provider jar " + jarFile, exception );
      }
    }

  public static Properties getProviderProperties( JarInputStream jarInputStream, boolean validate )
    {
    Properties providerProperties = getPropertiesResource( jarInputStream, ProviderDefinition.CASCADING_BIND_PROVIDER_PROPERTIES );

//    if( validate )
//      getProviderProperties( providerProperties );

    return providerProperties;
    }

  public static void getProviderProperties( Properties properties )
    {
    throw new UnsupportedOperationException( "unimplemented" );

/*
    String factoryClassName = properties.getProperty( ProviderDefinition.PROVIDER_FACTORY_CLASS_NAME );

    if( factoryClassName != null )
      {
      LOG.debug( "provider properties define factory {}", factoryClassName );
      return;
      }

    LOG.debug( "provider properties do not define {}", ProviderDefinition.PROVIDER_FACTORY_CLASS_NAME );
    Map<String, String> providerFormats = CatalogProviderUtil.getPropertiesWithSubstring( ProviderDefinition.PROVIDER_FORMAT, properties );
    Map<String, String> providerProtocols = CatalogProviderUtil.getPropertiesWithSubstring( ProviderDefinition.PROVIDER_PROTOCOL, properties );
    Map<String, String> providerStereotypes = CatalogProviderUtil.getPropertiesWithSubstring( ProviderDefinition.PROVIDER_STEREOTYPE, properties );

    if( LOG.isDebugEnabled() )
      {
      Object[] argArray = {providerFormats, providerProtocols, providerStereotypes};
      LOG.debug( "provider properties define formats: {}, protocols: {}, stereotypes: {}", argArray );
      }

    boolean hasFormats = ( providerFormats.size() > 0 ), hasProtocols = ( providerProtocols.size() > 0 ), hasStereotypes = ( providerStereotypes.size() > 0 );

    if( !hasFormats && !hasProtocols && !hasStereotypes )
      throw new InvalidProviderException( "provider jar does not specify factory class or stereotype/protocol/format classname properties" );
*/
    }
  }

