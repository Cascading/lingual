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

import java.io.File;
import java.io.FilenameFilter;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.WeakHashMap;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import com.jcabi.aether.Aether;
import org.apache.maven.artifact.repository.ArtifactRepository;
import org.apache.maven.project.MavenProject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sonatype.aether.artifact.Artifact;
import org.sonatype.aether.repository.RemoteRepository;
import org.sonatype.aether.util.artifact.DefaultArtifact;
import org.sonatype.aether.util.artifact.JavaScopes;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class CatalogProviderMavenUtil extends CatalogProviderUtil
  {
  private static final Logger LOG = LoggerFactory.getLogger( CatalogProviderMavenUtil.class );

  protected static Map< String, WeakReference<CatalogProviderMavenUtil> > instancesMaven =
      new WeakHashMap< String, WeakReference<CatalogProviderMavenUtil> >(); // by localRepositoryPath

  /**
   * @param bootstrapProperties   expected to contain CATALOG_ROOT_PATH_PROP, META_DATA_DIR_NAME_PROP, PROVIDER_DIR_NAME properties
   * @return instance
   */
  public static CatalogProviderMavenUtil getInstance( Properties bootstrapProperties )
    {
    return getInstance( bootstrapProperties, CatalogProviderMavenUtil.class, instancesMaven);
    }

  /**
   * Find and load jar artifact from a remote Maven repository into local one.
   * Loads *.jar and *.pom files.
   * Always loads dependencies (but not always returns them).
   * Based on http://www.jcabi.com/jcabi-aether/ wrapper around http://wiki.eclipse.org/Aether.
   *
   * @param signature             maven triplet, "groupId:artifactId:version", where version can be a range, e.g. [2.0,)
   * @param remoteRepositoryUrls  maven repositories to search
   * @param withDependencies      if false, returns single element list for the main jar, dependencies are loaded but not returned
   * @param overwrite             if false, skips loading for jars already present in localRepository
   * @param properties            properties that can help to resolve dependencies, e.g. containing jakarta-regexp.jakarta-regexp.version = 1.4, usually plugin's provider.properties
   * @return jar file in localRepository at [0], with dependencies following if requested, or null on error
   *
   * Hint: wrap result[0] into a JarFile to inspect/access elements.
   */
  public List<File> findAndLoadArtifactFromMavenRepo( String signature, List<String> remoteRepositoryUrls, boolean withDependencies, boolean overwrite, Properties properties )
    {
    LOG.debug( "loading jar: {}", signature );

    DefaultArtifact artifact = new DefaultArtifact( signature );
    List<Artifact> artifacts = null;

    try
      {
      if( !overwrite ) // check if exists
        {
        List<File> jarsDep = checkArtifactExistsInLocalRepo( signature, remoteRepositoryUrls, withDependencies, properties, new HashSet<String>() );
        if ( jarsDep != null && jarsDep.size() > 0 )
          return jarsDep;
        }

      Aether aether = initAether( remoteRepositoryUrls, localRepository );

      artifacts = aether.resolve( artifact, JavaScopes.RUNTIME );
      if( artifacts == null )
        throw new ArtifactResolutionException( "unexpected null from Aether.resolve on " + artifact );
      }
    catch( Exception e )
      {
      LOG.error( "cannot resolve: {}, with exception: {}", signature, e );
      return null;
      }

    List<File> jars = new ArrayList<File>();
    for (Artifact a : artifacts)
      {
      jars.add( a.getFile() );
      if( !withDependencies )
        break;
      }
    return jars;
    }

  /** Convenience method for single remoteRepositoryUrl */
  public List<File> findAndLoadArtifactFromMavenRepo(String signature, String remoteRepositoryUrl, boolean withDependencies, boolean overwrite, Properties properties)
    {
    return findAndLoadArtifactFromMavenRepo( signature, Collections.singletonList( remoteRepositoryUrl ), withDependencies, overwrite, properties );
    }

  /**
   * Checks if jar and ALL dependencies exist in local repository; downloads missing dependencies from maven repos.
   *
   * @param withDependencies  if false, ignore dependencies, check only the main jar
   * @param properties        properties that can help to resolve dependencies, e.g. containing jakarta-regexp:jakarta-regexp:version = 1.4, usually plugin's provider.properties
   *
   * @return local jars or null if jar or some dependencies do not exist in localRepository
   */
  protected List<File> checkArtifactExistsInLocalRepo( String signature, List<String> remoteRepositoryUrls, boolean withDependencies, Properties properties, Set<String> signaturesExisting ) throws ArtifactResolutionException
    {
    List<File> jars = new ArrayList<File>();

    DefaultArtifact artifact = new DefaultArtifact( signature );
    StringBuilder sb = new StringBuilder( localRepository.getAbsolutePath() );
    sb.append( '/' ).append( artifact.getGroupId().replace( '.', '/' ) ).append( '/' ).append( artifact.getArtifactId() );

    if( artifact.getVersion().contains( "," ) ) // version range - take the latest available // TODO: check that it is within range
      {
        File dir = new File( sb.toString() );
        File[] dirEntries = dir.listFiles();
        if( dirEntries == null )
          return null;
        List<String> dirList = new ArrayList<String>();
        for( File f: dirEntries )
          {
          if( f.isDirectory() )
            dirList.add( f.getName() );
          }
        Collections.sort( dirList );
        String version = dirList.get( dirList.size() - 1 );
        artifact.setVersion( version );
        LOG.debug( "version range {} is resolved to {}", signature, version );
        signature = signature.replace( artifact.getVersion(), version );
        artifact = new DefaultArtifact( signature );
      }

    sb.append( '/' ).append( artifact.getVersion() );

    File localArtifactDir = new File( sb.toString() );
    if( localArtifactDir.exists() )
      {
      // collect jar and pom names

      class FilenameFilterChecker implements FilenameFilter
        {
        String jarFileName = null, pomFileName = null;

        @Override
        public boolean accept(File dir, String name)
          {
          String nameLower = name.toLowerCase();
          if (nameLower.endsWith( ".jar" ))
            {
            jarFileName = name;
            return true;
            }
          if (nameLower.endsWith( ".pom" ))
            {
            pomFileName = name;
            return true;
            }
          return false;
          }
        }

      FilenameFilterChecker filter = new FilenameFilterChecker();
      File[] files = localArtifactDir.listFiles( filter ); // apply filter

      if( filter.jarFileName != null && ( !withDependencies || filter.pomFileName != null ) )
        {
        // we have the jar, but we might need dependencies, too
        String jarFilePath = localArtifactDir.getAbsolutePath() + '/' + filter.jarFileName;

        if( !withDependencies )
          {
          LOG.debug( "found existing artifact {}", jarFilePath );
          return Collections.singletonList( new File( jarFilePath ) );
          }
        else
          {
          LOG.debug( "checking dependencies for {}", signature );
          jars.add( new File( jarFilePath ) );
          // parse pom and check all the dependencies recursively
          String pomFilePath = localArtifactDir.getAbsolutePath() + '/' + filter.pomFileName;
          PomParser pomParser = new PomParser();
          if( pomParser.isGood() )
            {
            List<String> dependencySignatures = pomParser.getDependencies( new File(pomFilePath), artifact, properties );
            if( dependencySignatures == null )
              return null; // will cause loading of jar with dependencies
            LOG.debug( "dependencies for {}: {}", signature, dependencySignatures );
            for( String dependencySignature: dependencySignatures )
              {
              if( dependencySignature.indexOf( ' ' ) >=0 )
                dependencySignature = dependencySignature.replace( " ", "" );
              if( signaturesExisting.contains( dependencySignature ) )
                continue;
              List<File> jarsDep = checkArtifactExistsInLocalRepo( dependencySignature, remoteRepositoryUrls, withDependencies, properties, signaturesExisting );
              if( jarsDep == null )
                {
                jarsDep = findAndLoadArtifactFromMavenRepo( dependencySignature, remoteRepositoryUrls, withDependencies, true, properties );
                if( jarsDep == null )
                  {
                  LOG.debug( "existing artifact {} is found but some dependencies aren't, will download", signature );
                  return null;
                  }
                }
              jars.addAll( jarsDep );
              signaturesExisting.add( dependencySignature );
              }
            }
          else
            {
            LOG.error( "unexpected PomParser init failure" );
            return null;
            }
          LOG.debug( "found existing artifact {} with {} dependencies", jarFilePath, jars.size() - 1 );
          return jars;
          }
        }
      }

      LOG.debug( "existing artifact {} not found, download required", signature );
      return null;
    }

  /**
   * Helper for dependency extraction
   * Borrowed from http://affy.blogspot.com/2007/09/extracting-dependancy-lists-from-maven.html
   */
  public static class PomParser
    {
    private static XPathFactory xpathFactory = XPathFactory.newInstance();
    XPathExpression artifactIdExpr = null, dependencyExpr = null, depGroupIdExpr = null, depArtifactIdExpr = null, depVersionExpr = null, depScopeExpr = null, depOptionalExpr = null;
    boolean good = false;

    public PomParser()
      {
      try
        {
        init();
        good = true;
        } catch( Exception e )
        {
        LOG.error( "cannot init PomParser, with exception {}", e );
        }
      }

    public boolean isGood()
      {
      return good;
      }

    private void init() throws XPathExpressionException
      {
      XPath xpath = xpathFactory.newXPath();
      artifactIdExpr =    xpath.compile( "/project/artifactId/text()" );
      dependencyExpr =    xpath.compile( "/project/dependencies/dependency" );
      depGroupIdExpr =    xpath.compile( "./groupId/text()" );
      depArtifactIdExpr = xpath.compile( "./artifactId/text()" );
      depVersionExpr =    xpath.compile( "./version/text()" );
      depScopeExpr =      xpath.compile( "./scope/text()" );
      depOptionalExpr =   xpath.compile( "./optional/text()" );
      }

    /**
     * Extract dependencies from pom file.
     * @return list of dependency signatures (maven triplets)
     */
    public List<String> getDependencies( File pomFile, DefaultArtifact forArtifact, Properties properties ) throws ArtifactResolutionException
      {
      try
        {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse( pomFile );

        // If needed:
        // Node projectNode = document.getDocumentElement();
        // NodeList children = projectNode.getChildNodes();
        // String projectName = (String) artifactIdExpr.evaluate( document, XPathConstants.STRING );

        List<String> dependencies = new ArrayList<String>();
        NodeList nodes = (NodeList) dependencyExpr.evaluate( document, XPathConstants.NODESET );
        for( int i = 0; i < nodes.getLength(); i++ )
          {
          Node dependency = nodes.item( i );
          String val = (String) depScopeExpr.evaluate( dependency, XPathConstants.STRING );
          if( "test".equals( val ) ) // TODO: also exclude <scope>provided</scope> ??
            continue;
          val = (String) depOptionalExpr.evaluate( dependency, XPathConstants.STRING );
          if( "true".equals( val ) )
            continue;

          String depGroupId = (String) depGroupIdExpr.evaluate( dependency, XPathConstants.STRING );
          String depArtifactId = (String) depArtifactIdExpr.evaluate( dependency, XPathConstants.STRING );
          String depVersion = (String) depVersionExpr.evaluate( dependency, XPathConstants.STRING );

          if( depVersion.isEmpty() )
            {
            String propKey = depGroupId + '.' + depArtifactId + ".version";
            if( properties != null)
              depVersion = properties.getProperty( propKey, "" );
            if( depVersion.isEmpty() )
              {
              // TODO: check in maven central like this: http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22slf4j-api%22
              String msg = "version unknown, please supply " + propKey + " in provider.properties (required by " + pomFile + ")";
              LOG.error( msg );
              throw new ArtifactResolutionException( msg );
              }
              // LOG.debug( "FQN {}--{}", forArtifact, propKey );
            }
          String depSignature = depGroupId + ':' + depArtifactId + ':' + depVersion.trim();

          // substitute properties
          int p = 0, q = 0;
          String newDepSignature = null;
          while( ( p = depSignature.indexOf( "${", q ) ) >= 0 )
            {
              q = depSignature.indexOf( "}", p );
              if( q < 0 )
                throw new ArtifactResolutionException( "syntax error in '" + depSignature + "', file " + pomFile );
              String propName = depSignature.substring( p + 2, q );
              XPath xpath = xpathFactory.newXPath();
              XPathExpression propExpr = xpath.compile( "/project/properties/" + propName + "/text()" );
              String propVal = (String) propExpr.evaluate( document, XPathConstants.STRING );
              if( propVal.isEmpty() && (propName.startsWith( "project." ) || propName.startsWith( "pom." ) ) )
                {
                /* special variables from POM spec:
                ${project.build.directory} results in the path to your "target" directory, this is the same as ${pom.project.build.directory}
                ${project.build.outputDirectory} results in the path to your "target/classes" directory
                ${project.name}refers to the name of the project (deprecated: ${pom.name} ).
                ${project.version} refers to the version of the project (deprecated: or ${pom.version}).
                ${project.build.finalName} refers to the final name of the file created when the built project is packaged
                */
                String tagname = propName.substring( (propName.startsWith( "project." )) ? "project.".length() : "pom.".length() );
                propExpr = xpath.compile( "/project/" + tagname + "/text()" );
                propVal = (String) propExpr.evaluate( document, XPathConstants.STRING );
                if( propVal.isEmpty() )
                  {
                  propExpr = xpath.compile( "/project/parent/" + tagname + "/text()" );
                  propVal = (String) propExpr.evaluate( document, XPathConstants.STRING );
                  }
                if( propVal.isEmpty() ) // special cases heuristics (e.g. lucene-*)
                  {
                  if(propName.equals( "project.groupId" ))
                    propVal = forArtifact.getGroupId();
                  else if(propName.equals( "project.version" ))
                    propVal = forArtifact.getVersion();
                  }
                }

              if( propVal.isEmpty() ) {
                String propNameInProviderProperties = depSignature.replace( ':', '.' ) + '.' + propName;
                propVal = properties.getProperty( propNameInProviderProperties, "" );
                if( propVal.isEmpty() )
                  throw new ArtifactResolutionException( "property unknown: ${" + propName + "}, please supply " + propNameInProviderProperties + " in provider.properties (required by " + pomFile + ")" );
              }

              if( newDepSignature == null)
                newDepSignature = depSignature;
              newDepSignature = newDepSignature.replace( "${" + propName + "}", propVal.trim() );
              // TODO: properties could be defined in <parent>, add support for this
            }
          if( newDepSignature != null)
            {
            LOG.debug( "property substituted: {} -> {}", depSignature, newDepSignature );
            depSignature = newDepSignature;
            }

          dependencies.add( depSignature );
          }

        return dependencies;
        }
      catch (Exception e)
        {
        if( e instanceof ArtifactResolutionException )
          throw (ArtifactResolutionException) e;
        String msg = "cannot parse pom file " + pomFile;
        LOG.error( msg, e );
        throw new ArtifactResolutionException( msg, e );
        }
      }
    }

  /**
   * A substitute for new Aether(mavenProject,..)
   * @return Aether instance
   */
  private static Aether initAether(List<String> remoteRepositoryUrls, File localRepository)
    {
    List<RemoteRepository> remoteRepositories = new ArrayList<RemoteRepository>();
    for(String remoteRepositoryUrl: remoteRepositoryUrls)
      {
      RemoteRepository remoteRepository = new RemoteRepository( null, null, remoteRepositoryUrl );
      remoteRepository.setContentType( "default" ); // mark as default repo layout to avoid NoRepositoryConnectorException
      remoteRepositories.add( remoteRepository );
      }

    MavenProject project = new MavenProject();
    project.setRemoteArtifactRepositories( new ArrayList<ArtifactRepository>() ); // empty, replaced below

    Aether aether = new Aether( project, localRepository );

    setRemoteRepositories( aether, remoteRepositories.toArray( new RemoteRepository[0] ) ); // replacing
    // This is a hack to init MavenProject, which is used by Aether.<init> only as a container for
    // org.sonatype.aether.repository.RemoteRepository[]. The problem is MavenProject only supports
    // setRemoteArtifactRepositories(List<org.apache.maven.artifact.repository.ArtifactRepository>).
    // So we construct Aether with empty one and immediately replace it via reflection.
    // TODO: author notified and fixed, new release of jcabi-aether is coming in June 2013
    return aether;
    }

  /**
   * Helper for initAether()
   */
  private static void setRemoteRepositories(Aether aether, RemoteRepository[] remoteRepositories)
    {
    try
      {
      Field f = Aether.class.getDeclaredField( "remotes" );
      f.setAccessible( true );
      f.set( aether, remoteRepositories );
      } catch (Exception e)
      {
      throw new RuntimeException( "Unexpected Aether instantiation error", e );
      }
    }

  @SuppressWarnings("serial")
  static class ArtifactResolutionException extends Exception
    {
    ArtifactResolutionException( String message, Throwable cause )
      {
      super( message, cause );
      }
    ArtifactResolutionException( String message )
      {
      super( message );
      }
    }

  }
