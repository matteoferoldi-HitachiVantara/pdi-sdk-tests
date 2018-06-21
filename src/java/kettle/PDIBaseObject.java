package kettle;

import org.apache.log4j.Logger;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.RepositoryObjectType;

import java.io.File;

public class PDIBaseObject {

  protected static final Logger LOGGER = Logger.getLogger( PDIBaseObject.class );
  protected String name = "";
  protected String filePath = "";
  protected ObjectId objectId;
  protected RepositoryObjectType objType;
  protected static final long IMPLICIT_TIMEOUT = 30;
  protected static final long EXPLICIT_TIMEOUT = 30;

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public ObjectId getObjectId() {
    return objectId;
  }

  public void setObjectId( ObjectId objectId ) {
    this.objectId = objectId;
  }

  protected boolean isSaved() {
    boolean isSaved = false;

    if ( filePath.isEmpty() ) {
      return isSaved;
    }

    LOGGER.info( "Verifying if File exists" );
    File expectedFile = new File( filePath );
    if ( !expectedFile.exists() ) {
      isSaved = false;
    } else {
      isSaved = true;
    }

    return isSaved;
  }

  public void pause( long timeout ) {
    try {
      Thread.sleep( timeout * 1000 );
    } catch ( InterruptedException e ) {
      LOGGER.error( "Failed to do 'Thread.sleep' operation!", e );
    }
  }
}
