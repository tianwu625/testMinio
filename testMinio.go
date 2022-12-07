package main

import (
	"context"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var (
	rootak = ""
	rootsk = ""
	user1ak = ""
	user1sk = ""
	user2ak = ""
	user2sk = ""
)

func start(ctx context.Context, client *s3.Client, bucket, object string, reader io.Reader) error {
	if object == "" {
		input := s3.CreateBucketInput {
			Bucket:aws.String(bucket),
		}

		_, err := client.CreateBucket(ctx, &input);
		return err
	}
	input := s3.PutObjectInput {
		Bucket:aws.String(bucket),
		Key:aws.String(object),
		Body:reader,
	}
	_, err := client.PutObject(ctx, &input)
	return err
}

func finish(ctx context.Context, client *s3.Client, bucket, object string）error {
	if object == "" {
		input := s3.DeleteBucketInput {
			Bucket:aws.String(bucket),
		}
		_, err := client.DeleteBucket(ctx, &input)
		return err
	}

	input := s3.DeleteObjectInput {
		Bucket:aws.String(bucket),
		Key:aws.String(object),
	}

	_, err := client.DeleteObject(ctx, &input)
	return err
}

func createGrant(acl, atype, av string) types.Grant {
	var gtee types.Grantee
	switch atype {
	case "user":
		gtee.Type = types.TypeCanonicalUser
		gtee.ID = aws.String(av)
	case "group":
		gtee.Type = types.TypeGroup
		gtee.URI = aws.String(av)
	}

	var perm types.Permission
	switch acl {
	case "read":
		perm = PermissionRead
	case "aclread"
		perm = PermissionReadAcp
	case "aclwrite"
		perm = PermissionWriteAcp
	case "write"
		perm = PermissionWrite
	case "full"
		perm = PermissionFullControl
	}
	grant := types.Grant {
		Grantee: &gtee,
		Permission:perm,
	}

	return grant
}



type aclelem struct {
	atype string
	acl string
	av string
}

func createAclPolicy(...aclelems aclelem) *types.AccessControlPolicy {
	cp := types.AccessControlPolicy{}
	var grants []types.Grant
	for _, a := range aclelems {
		grants = append(grants, createGrant(a.acl, a.atype, a.av)
	}
	cp.Grants = grants

	return &cp
}

var (
	cidroot = ""
	ciduser1 = ""
	ciduser2 = ""
)

func setAcl(ctx context.Context, client *s3.Client, bucket, object, acl string) error {
	var acp types.AccessControlPolicy
	switch acl {
	case "read":
		e := aclelem {
			atype:"user",
			acl: "read",
			av:ciduser1,
		}
		acp = createAclPolicy(e)
	case "write":
		e := aclelem {
			atype:"user",
			acl:"write",
			av:ciduser1,
		}
		acp = createAclPolicy(e)
	case "aclread":
		e := aclelem {
			atype:"user",
			acl:"aclread",
			av:ciduser1,
		}
		acp = createAclPolicy(e)
	case "aclwrite":
		e := aclelem {
			atype:"user",
			acl:"aclwrite",
			av:ciduser1,
		}
		acp = createAclPolicy(e)
	}

	if object == "" {
		input := s3.PutBucketAclInput {
			Bucket:aws.String(bucket),
			AccessControlPolicy:acp,
		}
		_, err := client.PutBucketAcl(ctx, &input)
		return err
	}
	input := s3.PutObjectAclInput {
		Bucket:aws.String(bucket),
		AccessControlPolicy:acp,
		Key:aws.String(object),
	}
	_, err := client.PutObjectAcl(ctx, &input)
	return err
}

func getAcl(ctx context.Context, client *s3.Client, bucket, object string) error {
	if object == "" {
		input := s3.GetBucketAclInput {
			Bucket:aws.String(bucket),
		}

		_, err := client.GetBucketAcl(ctx, &input)
		return err
	}

	input := s3.GetObjectAclInput {
		Bucket:aws.String()
	}
}

func getS3client(ctx context.Context, ak, sk string) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"),
		   config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, sk, "")),
	           config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string,
	           options ...interface{})(aws.Endpoint, error){
			   return aws.Endpoint{
				   URL: "http://192.168.21.164:9000",
				   HostnameImmutable: true,
			   }, nil
		   })))
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(cfg)

	return client, nil

}

func testAclReadPermissionPass() error {
	return nil
}

func testAclReadPermissionFail() error {
	return nil
}

func testAclWritePermissionPass() error {
	return nil
}

func testAclWritePermissionFail() error {
	return nil
}

func testReadPermissionPass() error {
	return nil
}

func testReadPermissionFail() error {
	return nil
}

func testWritePermissionPass() error {
	return nil
}

func testWritePermissionFail() error {
	return nil
}

func testAclReadObjectPermissionPass error {
}

func testAclReadObjectPermissionFail error {
}

func test

var testcases = map[string]func()error {
	"testAclReadPermissionPass": testAclReadPermissionPass,
	"testAclReadPermissionFail": testAclReadPermissionFail,
	"testAclWritePermissionPass": testAclWritePermissionPass,
	"testAclWritePermissionFail": testAclWritePermissionFail,
	"testReadPermissionPass":testReadPermissionPass,
	"testReadPermissionFail":testReadPermissionFail,
	"testWritePermissionPass":testWritePermissioPass,
	"testWritePermissionFail": testWritePermissionFail,
	"testAclReadObjectPermissionPass": testAclReadObjectPermissionPass,
	"testAclReadObjectPermissionFail": testAclReadObjectPermissionFail,
	"testAclWriteObjectPermissionPass": testAclWriteObjectPermissionPass,
	"testAclWriteObjectPermissionFail": testAclWriteObjectPermissionFail,
	"testReadObjectPass": testReadObjectPass,
	"testReadObjectFail": testReadObjectFail,
	"testNewMultiPartPass": testNewMultiPartPass,
	"testNewMultiPartFail":testNewMultiPartFail,
	"testUploadPartPass": testUploadPartPass,
	"testUploadPartFail": testUploadPartFail,
	"testCompleteMultiPartPass": testCompleteMultiPartPass,
	"testCompleteMultiPartFail": testCompleteMultiPartFail,
	"testListMultiPartPass": testListMultiPartPass,
	"testListMultiPartFail": testListMultiPartFail,
	"testCopyObjectPass": testCopyObjectPass,
	"testCopyObjectFail": testCopyObjectFail,
	"testUploadCopyPass": testUploadCopyPass,
	"testUploadCopyFail": testUploadCopyFail,
	"testSelectContentPass": testSelectContentPass,
	"testSelectContentFail": testSelectContentFail,
	"testheadBucketPass": testheadBucketPass,
	"testheadBucketFail": testheadBucketFail,
	"testheadObjectPass": testheadObjectPass,
	"testheadObjectFail": testheadObjectFail,
	"testListObjectsPass": testListObjectsPass,
	"testListObjectsFail": testListObjectsFail,
}

func main() {
	for name, f := range testcases {
		if err := f(); err != nil {
			fmt.Printf("%30v fail", name)
			continue
		}
		fmt.Printf("%30v pass", name)
	}
}
