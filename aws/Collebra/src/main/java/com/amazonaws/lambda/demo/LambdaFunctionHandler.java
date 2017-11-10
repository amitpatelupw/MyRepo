package com.amazonaws.lambda.demo;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;

public class LambdaFunctionHandler implements RequestHandler<Object, String> {

    @Override
    public String handleRequest(Object input, Context context) {
    	
    	try
    	{
    	HttpClient client = HttpClientBuilder.create().build();

        List<NameValuePair> postParameters = new ArrayList<NameValuePair>();

         postParameters.add(new BasicNameValuePair("tableViewConfig",  "{\"TableViewConfig\":{\"displayLength\":-1,\"displayStart\":0,\"Resources\":{\"Term\":{\"Signifier\":{\"name\":\"termsignifier\"},\"Id\":{\"name\":\"termrid\"},\"ConceptType\":[{\"Signifier\":{\"name\":\"concepttypename\"},\"Id\":{\"name\":\"concepttyperid\"}}],\"Status\":{\"Signifier\":{\"name\":\"statusname\"},\"Id\":{\"name\":\"statusrid\"}},\"Vocabulary\":{\"Id\":{\"name\":\"domainrid\"},\"Name\":{\"name\":\"domainname\"},\"Community\":{\"Id\":{\"name\":\"commrid\"},\"Name\":{\"name\":\"communityname\"}},\"VocabularyType\":{\"Id\":{\"name\":\"domaintyperid\"},\"Signifier\":{\"name\":\"domaintypename\"}}},\"StringAttribute\":[{\"Id\":{\"name\":\"Attrf4ed31a499f14b1788e646763821a485rid\"},\"labelId\":\"f4ed31a4-99f1-4b17-88e6-46763821a485\",\"Value\":{\"name\":\"Attrf4ed31a499f14b1788e646763821a485\"},\"LongExpression\":{\"name\":\"Attrf4ed31a499f14b1788e646763821a485longExpr\",\"stripHtml\":true}}],\"SingleValueListAttribute\":[{\"labelId\":\"0dc4a317-37d7-426b-9327-10a990794aa1\",\"Value\":{\"name\":\"Attr0dc4a31737d7426b932710a990794aa1\"}}],\"NumericAttribute\":[{\"labelId\":\"9dfb883f-b109-4b5d-8bdc-941725c331c8\",\"NumericValue\":{\"name\":\"Attr9dfb883fb1094b5d8bdc941725c331c8\"}}],\"DateAttribute\":{\"Date\":{\"name\":\"Attr1ecee3428b5f463cabfffcb2dda9e903\"},\"labelId\":\"1ecee342-8b5f-463c-abff-fcb2dda9e903\"},\"Relation\":[{\"typeId\":\"ccdf950d-03b3-4454-a87e-2e783205fd26\",\"type\":\"SOURCE\",\"Target\":{\"Signifier\":{\"name\":\"Relccdf950d03b34454a87e2e783205fd26S\"},\"Id\":{\"name\":\"Relccdf950d03b34454a87e2e783205fd26Srid\"},\"ConceptType\":{\"Signifier\":{\"name\":\"Relccdf950d03b34454a87e2e783205fd26Sct\"}},\"Vocabulary\":{\"Name\":{\"name\":\"Relccdf950d03b34454a87e2e783205fd26Svc\"},\"VocabularyType\":{\"Signifier\":{\"name\":\"Relccdf950d03b34454a87e2e783205fd26Svt\"}},\"Community\":{\"Name\":{\"name\":\"Relccdf950d03b34454a87e2e783205fd26Sco\"}}}}}],\"Filter\":{\"AND\":[{\"AND\":[{\"Field\":{\"name\":\"concepttyperid\",\"operator\":\"EQUALS\",\"value\":\"8806279e-6f0a-4f86-b069-f395f2ac8f4f\"}}]}]},\"Order\":[{\"Field\":{\"name\":\"termsignifier\",\"order\":\"asc\"}}]}},\"Columns\":[{\"Group\":{\"name\":\"agroup\",\"Columns\":[{\"Column\":{\"label\":\"Name\",\"fieldName\":\"termsignifier\"}}]}},{\"Group\":{\"name\":\"agroup\",\"Columns\":[{\"Column\":{\"label\":\"AssetID\",\"fieldName\":\"termrid\"}}]}},{\"Group\":{\"name\":\"agroup\",\"Columns\":[{\"Column\":{\"label\":\"Type\",\"fieldName\":\"concepttypename\"}}]}},{\"Group\":{\"name\":\"agroup\",\"Columns\":[{\"Column\":{\"label\":\"Status\",\"fieldName\":\"statusname\"}}]}},{\"Group\":{\"name\":\"agroup\",\"Columns\":[{\"Column\":{\"label\":\"Domain\",\"fieldName\":\"domainname\"}}]}},{\"Group\":{\"name\":\"agroup\",\"Columns\":[{\"Column\":{\"label\":\"ExecutionDate\",\"fieldName\":\"Attr1ecee3428b5f463cabfffcb2dda9e903\"}}]}},{\"Group\":{\"name\":\"agroup\",\"Columns\":[{\"Column\":{\"label\":\"ProfilingTechnique\",\"fieldName\":\"Attr0dc4a31737d7426b932710a990794aa1\"}}]}},{\"Group\":{\"name\":\"agroup\",\"Columns\":[{\"Column\":{\"label\":\"ProfileRuleDescription(NoFormatting)\",\"fieldName\":\"Attrf4ed31a499f14b1788e646763821a485longExpr\"}}]}},{\"Group\":{\"name\":\"agroup\",\"Columns\":[{\"Column\":{\"label\":\"Total#RecordsProfiled\",\"fieldName\":\"Attr9dfb883fb1094b5d8bdc941725c331c8\"}}]}},{\"Column\":{\"label\":\"Relatedto(DataElement)[DataElement]>DataElement\",\"fieldName\":\"Relccdf950d03b34454a87e2e783205fd26S\"}},{\"Column\":{\"label\":\"Relatedto(DataElement)[DataElement]>Type\",\"fieldName\":\"Relccdf950d03b34454a87e2e783205fd26Sct\"}},{\"Column\":{\"label\":\"Relatedto(DataElement)[DataElement]>Domain\",\"fieldName\":\"Relccdf950d03b34454a87e2e783205fd26Svc\"}},{\"Column\":{\"label\":\"Relatedto(DataElement)[DataElement]>DomainType\",\"fieldName\":\"Relccdf950d03b34454a87e2e783205fd26Svt\"}},{\"Column\":{\"label\":\"Relatedto(DataElement)[DataElement]>Community\",\"fieldName\":\"Relccdf950d03b34454a87e2e783205fd26Sco\"}},{\"Group\":{\"name\":\"agroup\",\"Columns\":[{\"Column\":{\"label\":\"DomainType\",\"fieldName\":\"domaintypename\"}}]}},{\"Group\":{\"name\":\"agroup\",\"Columns\":[{\"Column\":{\"label\":\"Community\",\"fieldName\":\"communityname\"}}]}}]}}"));

            


         HttpPost request =  new HttpPost("https://dtcc-edm-dev.collibra.com/rest/latest/output/csv-file");

         request.setHeader("Content-Type", "application/x-www-form-urlencoded");

         request.setHeader("Authorization", "Basic YXJ1bjphcnVuQDA1MTc=");

         request.setEntity(new UrlEncodedFormEntity(postParameters,"UTF-8"));

         HttpResponse response = client.execute(request);

         String responseBody = EntityUtils.toString(response.getEntity());

         System.out.println(responseBody);
         
         ObjectMetadata meta = new ObjectMetadata();
         meta.setContentMD5(new String(com.amazonaws.util.Base64.encode(DigestUtils.md5(responseBody))));
         meta.setContentLength(responseBody.length());
         InputStream stream = new ByteArrayInputStream(responseBody.getBytes(StandardCharsets.UTF_8));               
 		@SuppressWarnings("deprecation")
		AmazonS3 s3client = new AmazonS3Client(new ProfileCredentialsProvider());
         PutObjectResult  objS =  s3client.putObject("bdemo-bucket", "AKIAJMKTQ6PAFBSLZNPA", stream, meta);
    	}
    	catch(Exception ex)
    	{
    		
    	}

        // TODO: implement your handler
        return "Data saved successfully on s3";
    }

}
