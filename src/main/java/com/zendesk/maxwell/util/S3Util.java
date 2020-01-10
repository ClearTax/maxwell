package com.zendesk.maxwell.util;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URL;

public class S3Util {
	private  static final Logger logger = LoggerFactory.getLogger(S3Util.class);

	private final AmazonS3 amazonS3;

	public S3Util(final AmazonS3 amazonS3) {
		this.amazonS3 = amazonS3;
	}

	public URL upload(final String bucketName, final String keyName, final String content) {
		logger.debug("Uploading file {} to bucket {}", keyName, bucketName);

		final ObjectMetadata objectMetadata = new ObjectMetadata();

		try {
			objectMetadata.setContentLength(content.getBytes("UTF-8").length);
		} catch (final UnsupportedEncodingException e) {
			logger.error("Unsupported encoding exception while uploading to s3!", e);
			throw new RuntimeException(e);
		}

		PutObjectRequest putObjectRequest = new PutObjectRequest(
				bucketName,
				keyName,
				IOUtils.toInputStream(content),
				objectMetadata
		);
		amazonS3.putObject(putObjectRequest);

		return getUrl(bucketName, keyName);
	}

	private URL getUrl(final String bucketName, final String keyName) {
		return amazonS3.getUrl(bucketName, keyName);
	}
}
