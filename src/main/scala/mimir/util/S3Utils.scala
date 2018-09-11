package mimir.util

import java.io.ByteArrayInputStream
import java.io.File
import java.io.InputStream
import java.util.List
import my.com.amazonaws.HttpMethod;
import my.com.amazonaws.auth.AWSCredentials
import my.com.amazonaws.auth.BasicAWSCredentials
import my.com.amazonaws.auth.AWSStaticCredentialsProvider
import my.com.amazonaws.auth.profile.ProfileCredentialsProvider
import my.com.amazonaws.services.s3.AmazonS3
import my.com.amazonaws.services.s3.AmazonS3Client
import my.com.amazonaws.services.s3.model.Bucket
import my.com.amazonaws.services.s3.model.CannedAccessControlList
import my.com.amazonaws.services.s3.model.ObjectMetadata
import my.com.amazonaws.services.s3.model.PutObjectRequest
import my.com.amazonaws.services.s3.model.S3ObjectSummary
import my.com.amazonaws.services.s3.AmazonS3ClientBuilder
import my.com.amazonaws.services.s3.model.GeneratePresignedUrlRequest
import scala.collection.JavaConversions._
import java.net.HttpURLConnection
import java.io.OutputStreamWriter
import java.net.URL
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.BufferedOutputStream
import java.io.BufferedInputStream
import my.com.amazonaws.services.s3.model.GetObjectRequest
import my.com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration

object S3Utils {
  private val PATH_SEP: String = "/"

  /**
   * @param accessKeyID
   * @param secretAccessKey
   * @param clientRegion
   * @return
   */
  def authenticate(accessKeyID: String, secretAccessKey: String, clientRegion: String, endpoint:Option[String] = None): AmazonS3 = {
    // credentials object identifying user for authentication
    // user must have AWSConnector and AmazonS3FullAccess for
    // credentials object identifying user for authentication
    // user must have AWSConnector and AmazonS3FullAccess for 
    // this to work
    val credentials: AWSCredentials =
      new BasicAWSCredentials(accessKeyID, secretAccessKey)
    // create a client connection based on credentials
    //new AmazonS3Client(credentials)
        
    val clientBuilder = AmazonS3ClientBuilder.standard()
      .withCredentials(new AWSStaticCredentialsProvider(credentials)) //new ProfileCredentialsProvider())
      .withRegion(clientRegion)
    
      endpoint.flatMap(ep => {
        val endpointConfiguration = new EndpointConfiguration(ep, null);
        clientBuilder.setEndpointConfiguration(endpointConfiguration)
        Some(clientBuilder)
      })
      .getOrElse(clientBuilder)
      .build();
  }

  /**
   * @param bucketName
   * @param s3client
   * @return
   */
  def createBucket(bucketName: String,
                   s3client: AmazonS3) = {
    s3client.createBucket(bucketName)
  }

  /**
   * @param bucketName
   * @param s3client
   */
  def deleteBucket(bucketName: String,
                   s3client: AmazonS3) = {
    s3client.deleteBucket(bucketName)
  }

  /**
   * @param s3client
   * @return
   */
  def bucketsList(s3client: AmazonS3): Seq[String] = {
    s3client.listBuckets().map(_.getName)
  }

  /**
   * @param bucketName
   * @param folderName
   * @param client
   */
  def createFolder(bucketName: String, folderName: String, s3client: AmazonS3): Unit = {
    // create meta-data for your folder and set content-length to 0
    val metadata: ObjectMetadata = new ObjectMetadata()
    metadata.setContentLength(0)
    // create empty content
    val emptyContent: InputStream = new ByteArrayInputStream(
      Array.ofDim[Byte](0))
    // create a PutObjectRequest passing the folder name suffixed by /
    val putObjectRequest: PutObjectRequest = new PutObjectRequest(
      bucketName,
      folderName + PATH_SEP,
      emptyContent,
      metadata)
    // send request to S3 to create folder
    s3client.putObject(putObjectRequest)
    
    println("folder created: " + folderName)
  }
  
  /**
   * @param bucketName
   * @param folderPath
   * @param s3client
   */
  def createFolderAndParents(bucketName: String, folderPath: String, s3client: AmazonS3): Unit = {
    val targetPaths = folderPath.split(File.separator)
    createFolderAndParents(bucketName, targetPaths, s3client)
  }
  
  /**
   * @param bucketName
   * @param folderPath
   * @param s3client
   */
  def createFolderAndParents(bucketName: String, folderPath: Array[String], s3client: AmazonS3): Unit = {
    folderPath.foldLeft("")((init, cur) => {
      val curDir = init + cur
      if(cur.isEmpty()){
        init
      }
      else if(objectExists(bucketName, curDir , s3client)){
        curDir + PATH_SEP
      }
      else{
        createFolder(bucketName, curDir, s3client)
        curDir + PATH_SEP
      }
    })
  }

  /**
   * This method first deletes all the files in given folder and than the
   * folder itself
   * 
   * @param bucketName
   * @param folderName
   * @param client
   */
  def deleteFolder(bucketName: String, folderName: String, client: AmazonS3): Unit = {
    val fileList =
      client.listObjects(bucketName, folderName).getObjectSummaries
    for (file <- fileList) {
      client.deleteObject(bucketName, file.getKey)
    }
    client.deleteObject(bucketName, folderName)
  }

  /**
   * @param bucketName
   * @param folderName
   * @param srcFile
   * @param targetFile
   * @param s3client
   * @return
   */
  def uploadFile(bucketName: String, srcFile: String, targetFile: String, s3client: AmazonS3, overwrite:Boolean = false) = {
    if(!objectExists(bucketName, targetFile, s3client) || overwrite){
      // upload file to folder and set it to public
      s3client.putObject(
        new PutObjectRequest(bucketName,
          targetFile,
          new File(srcFile))
          .withCannedAcl(CannedAccessControlList.Private))
    }
  }
  
  /**
   * @param bucketName
   * @param objKey
   * @param s3client
   * @return
   */
  def objectExists(bucketName: String, objKey: String, s3client: AmazonS3): Boolean = {
    s3client.doesObjectExist(bucketName, objKey);
  }
  
  /**
   * @param bucketName
   * @param srcFile
   * @param targetFile
   * @param s3client
   * @param overwrite
   * @return
   */
  def copyToS3(bucketName: String, srcFile: String, targetFile: String, s3client: AmazonS3, overwrite:Boolean = false) = {
    if(!objectExists(bucketName, targetFile, s3client) || overwrite){
      // Set the pre-signed URL to expire after one hour.
      val expiration: java.util.Date = new java.util.Date()
      var expTimeMillis: Long = expiration.getTime
      expTimeMillis += 1000 * 60 * 60
      expiration.setTime(expTimeMillis)
      // Generate the pre-signed URL.
      val generatePresignedUrlRequest: GeneratePresignedUrlRequest =
        new GeneratePresignedUrlRequest(bucketName, targetFile)
          .withMethod(HttpMethod.PUT)
          .withExpiration(expiration)
      val url: URL = s3client.generatePresignedUrl(generatePresignedUrlRequest)
      // Create the connection and use it to upload the new object using the pre-signed URL.
      val connection: HttpURLConnection =
        url.openConnection().asInstanceOf[HttpURLConnection]
      connection.setDoOutput(true)
      connection.setRequestMethod("PUT")
      val out = new BufferedOutputStream(connection.getOutputStream)
      
      val sourceFile = new File(srcFile)
      val srcUrl = if(sourceFile.getPath.contains(":/")) new java.net.URL(sourceFile.getPath.replaceFirst(":/", "://")) else sourceFile.toURI().toURL()
      val input = new BufferedInputStream(srcUrl.openStream)
      
      val bytes = new Array[Byte](1024) //1024 bytes - Buffer size
      Iterator
      .continually (input.read(bytes))
      .takeWhile (_ != -1L)
      .foreach (read=>out.write(bytes,0,read))
      
      out.flush()
      out.close()
      connection.getResponseCode
    }
  }
  
  def copyToS3Stream(bucketName: String, input:InputStream, targetFile: String, s3client: AmazonS3, overwrite:Boolean = false) = {
    if(!objectExists(bucketName, targetFile, s3client) || overwrite){
      // Set the pre-signed URL to expire after one hour.
      val expiration: java.util.Date = new java.util.Date()
      var expTimeMillis: Long = expiration.getTime
      expTimeMillis += 1000 * 60 * 60
      expiration.setTime(expTimeMillis)
      // Generate the pre-signed URL.
      val generatePresignedUrlRequest: GeneratePresignedUrlRequest =
        new GeneratePresignedUrlRequest(bucketName, targetFile)
          .withMethod(HttpMethod.PUT)
          .withExpiration(expiration)
      val url: URL = s3client.generatePresignedUrl(generatePresignedUrlRequest)
      // Create the connection and use it to upload the new object using the pre-signed URL.
      val connection: HttpURLConnection =
        url.openConnection().asInstanceOf[HttpURLConnection]
      connection.setDoOutput(true)
      connection.setRequestMethod("PUT")
      val out = new BufferedOutputStream(connection.getOutputStream)
      
      val bytes = new Array[Byte](1024) //1024 bytes - Buffer size
      Iterator
      .continually (input.read(bytes))
      .takeWhile (_ != -1L)
      .foreach (read=>out.write(bytes,0,read))
      
      out.flush()
      out.close()
      connection.getResponseCode
    }
  }
  
  def readFromS3(bucketName: String, key: String, s3client: AmazonS3): InputStream = {
    val s3object =
      s3client.getObject(new GetObjectRequest(bucketName, key))
    s3object.getObjectContent
  }
}