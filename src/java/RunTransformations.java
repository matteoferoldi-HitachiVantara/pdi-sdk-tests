import kettle.PDIJob;
import kettle.PDITransformation;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.job.Job;
import org.pentaho.di.trans.Trans;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;
import sun.rmi.runtime.Log;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class RunTransformations extends SDKBaseTest {

  private static final String PENTAHO_HOME = "/Applications/Pentaho";
  private static final String PDI_SAMPLES_HOME = PENTAHO_HOME + "/design-tools/data-integration/samples/";
  private SoftAssert softAssert = new SoftAssert();

  @Test
  public void run() {
    // Map: parameter, value
    Map<String, String> parameters = new HashMap<String, String>();

    // Example with parameters
    parameters.put( "NR_OF_ROWS", "15" );
    runTransformation( "data-generator\\Generate customer data.ktr", parameters );

    runJobs( "job1.kjb" );

    softAssert.assertAll();
  }

  public void runTransformation( String transformationPath ) {
    runTransformation( transformationPath, null, LogLevel.BASIC );
  }

  public void runTransformation( String transformationPath, LogLevel level ) {
    runTransformation( transformationPath, null, level );
  }

  public void runTransformation( String transformationPath, Map<String, String> parameters ) {
    runTransformation( transformationPath, parameters, LogLevel.BASIC );
  }

  public void runTransformation( String transformationPath, Map<String, String> parameters, LogLevel level ) {

    System.out.println( "Starting Transformation...\n" );
    String transFilePath = PDI_SAMPLES_HOME + "transformations/" + transformationPath;
    PDITransformation trans = new PDITransformation( transFilePath, null );

    if ( parameters != null && parameters.size() > 0 ) {
      for ( String parameter : parameters.keySet() ) {
        trans.setParameter( parameter, parameters.get( parameter ) );
      }
    }

    Trans transExec = trans.runTransformation( level );
    String logText = getPDILog( transExec );

    softAssert.assertTrue( transExec.getResult().getResult() && transExec.getResult().getNrErrors() == 0,
      "Transformation failed. Number of Errors: " + transExec.getResult().getNrErrors() );
  }

  public void runJobs( String jobPath ) {
    runJobs( jobPath, LogLevel.BASIC );
  }

  public void runJobs( String jobPath, LogLevel level ) {
    String jobFilePath = PDI_SAMPLES_HOME + "jobs/" + jobPath;
    PDIJob job = new PDIJob( jobFilePath, null );

    System.out.println( "Running Job...\n" );
    Job jobExec = job.run( level );
    String logText = getPDILog( jobExec );

    softAssert.assertTrue( jobExec.getResult().getResult(),
      "Job failed. Number of Errors: " + jobExec.getResult().getNrErrors() );

  }
}
