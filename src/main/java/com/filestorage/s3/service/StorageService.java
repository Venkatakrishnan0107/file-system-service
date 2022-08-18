package com.filestorage.s3.service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

@Service
public class StorageService {

    @Value("${application.bucket.name}")
    private String bucketName;

    @Qualifier("generateS3Client")
    @Autowired
    private AmazonS3 s3Client;

    public String uploadFile(MultipartFile multipartFile)  {
        String fileName = multipartFile.getOriginalFilename();
        File file  = null;
        try {
            file = getConvertedFile(multipartFile);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("image/jpeg");
        PutObjectResult result = s3Client.putObject(new PutObjectRequest(bucketName,fileName,file).withMetadata(metadata));
        s3Client.setObjectAcl(bucketName,fileName,CannedAccessControlList.PublicRead);
        System.out.println("Url..."+s3Client.getUrl(bucketName,fileName));
        file.delete();
        return "File Uploaded "+fileName;
    }

    public byte[] downloadFile(String fileName){
        S3Object s3Object = s3Client.getObject(bucketName,fileName);
        S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent();
        try {
            byte[] bytes = IOUtils.toByteArray(s3ObjectInputStream);
            return bytes;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String deleteFile(String fileName){
        s3Client.deleteObject(bucketName,fileName);
        return "File removed";
    }

    private File getConvertedFile(MultipartFile multipartFile) throws FileNotFoundException {
        File convertedFile = new File(multipartFile.getOriginalFilename());
        try(FileOutputStream stream = new FileOutputStream(convertedFile)){
            stream.write(multipartFile.getBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return convertedFile;
    }
}
