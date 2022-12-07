package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var (
	rootak  = "zDIYDh8ZtkTWQY4J"
	rootsk  = "UCnmZDWa40YJnlj30L2572T8qgZM8koW"
	user1ak = "VoihHdTLxi9asgIh"
	user1sk = "O7y7MmVD28F3B2tIqNXN1LYJV5lSMLDE"
	user2ak = "yQ6ARsAu5PxSO9l8"
	user2sk = "QQ1zX9Hbzj1KDc08TUyV6Zx1yRjB5txJ"
)

func start(ctx context.Context, client *s3.Client, bucket, object string, reader io.Reader) error {
	if object == "" {
		input := s3.CreateBucketInput{
			Bucket: aws.String(bucket),
		}

		_, err := client.CreateBucket(ctx, &input)
		return err
	}
	input := s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
		Body:   reader,
	}
	_, err := client.PutObject(ctx, &input)
	return err
}

func finish(ctx context.Context, client *s3.Client, bucket, object string) error {
	if object == "" {
		input := s3.DeleteBucketInput{
			Bucket: aws.String(bucket),
		}
		_, err := client.DeleteBucket(ctx, &input)
		return err
	}

	input := s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
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
		perm = types.PermissionRead
	case "aclread":
		perm = types.PermissionReadAcp
	case "aclwrite":
		perm = types.PermissionWriteAcp
	case "write":
		perm = types.PermissionWrite
	case "full":
		perm = types.PermissionFullControl
	}
	grant := types.Grant{
		Grantee:    &gtee,
		Permission: perm,
	}

	return grant
}

type aclelem struct {
	atype string
	acl   string
	av    string
}

func createAclPolicy(elems ...aclelem) *types.AccessControlPolicy {
	cp := types.AccessControlPolicy{}
	var grants []types.Grant
	for _, a := range elems {
		grant := createGrant(a.acl, a.atype, a.av)
		grants = append(grants, grant)
	}
	cp.Grants = grants

	return &cp
}

var (
	ciduser1 = "6f70656e66734843573243574e4c4235444e55563344444f364330344a504239"
	ciduser2 = "6f70656e66734247313654324e33464838374530524448385548543652463157"
)

func setAcl(ctx context.Context, client *s3.Client, bucket, object, acl string) error {
	var acp *types.AccessControlPolicy
	switch acl {
	case "read":
		e := aclelem{
			atype: "user",
			acl:   "read",
			av:    ciduser1,
		}
		acp = createAclPolicy(e)
	case "write":
		e := aclelem{
			atype: "user",
			acl:   "write",
			av:    ciduser1,
		}
		acp = createAclPolicy(e)
	case "aclread":
		e := aclelem{
			atype: "user",
			acl:   "aclread",
			av:    ciduser1,
		}
		acp = createAclPolicy(e)
	case "aclwrite":
		e := aclelem{
			atype: "user",
			acl:   "aclwrite",
			av:    ciduser1,
		}
		acp = createAclPolicy(e)
	}

	if object == "" {
		input := s3.PutBucketAclInput{
			Bucket:              aws.String(bucket),
			AccessControlPolicy: acp,
		}
		_, err := client.PutBucketAcl(ctx, &input)
		return err
	}
	input := s3.PutObjectAclInput{
		Bucket:              aws.String(bucket),
		AccessControlPolicy: acp,
		Key:                 aws.String(object),
	}
	_, err := client.PutObjectAcl(ctx, &input)
	return err
}

func getAcl(ctx context.Context, client *s3.Client, bucket, object string) error {
	if object == "" {
		input := s3.GetBucketAclInput{
			Bucket: aws.String(bucket),
		}

		_, err := client.GetBucketAcl(ctx, &input)
		return err
	}

	input := s3.GetObjectAclInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}

	_, err := client.GetObjectAcl(ctx, &input)
	return err
}

func getS3client(ctx context.Context, ak, sk string) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, sk, "")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string,
			options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:               "http://192.168.21.164:9000",
				HostnameImmutable: true,
			}, nil
		})))
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(cfg)

	return client, nil

}

var (
	letters = []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	kb      = 1024
	mb      = (1024 * 1024)
	gb      = (1024 * 1024 * 1024)
)

func createfile(path string, size int) error {
	content := []byte{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < size; i++ {
		content = append(content, letters[r.Intn(len(letters))])
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}

	defer f.Close()
	if _, err := f.Write(content); err != nil {
		return err
	}
	return nil
}

func remove(path string) error {
	return os.Remove(path)
}

var (
	tbuck = "testbuck"
	tfile = "testfile"
	snil  = ""
)

func testAclReadPermissionPass() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := setAcl(ctx, rootclient, tbuck, snil, "aclread"); err != nil {
		return err
	}
	user1client, err := getS3client(ctx, user1ak, user1sk)
	if err != nil {
		return err
	}
	if err := getAcl(ctx, user1client, tbuck, snil); err != nil {
		return err
	}
	return nil
}

var (
	errExpectFail = errors.New("should be fail")
)

func testAclReadPermissionFail() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := setAcl(ctx, rootclient, tbuck, snil, "aclread"); err != nil {
		return err
	}
	user2client, err := getS3client(ctx, user2ak, user2sk)
	if err != nil {
		return err
	}
	if err := getAcl(ctx, user2client, tbuck, snil); err != nil {
		return nil
	}
	return errExpectFail
}

func testAclWritePermissionPass() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := setAcl(ctx, rootclient, tbuck, snil, "aclwrite"); err != nil {
		return err
	}
	user1client, err := getS3client(ctx, user1ak, user1sk)
	if err != nil {
		return err
	}
	if err := setAcl(ctx, user1client, tbuck, snil, "aclwrite"); err != nil {
		return err
	}
	return nil
}

func testAclWritePermissionFail() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := setAcl(ctx, rootclient, tbuck, snil, "aclwrite"); err != nil {
		return err
	}
	user2client, err := getS3client(ctx, user2ak, user2sk)
	if err != nil {
		return err
	}
	if err := setAcl(ctx, user2client, tbuck, snil, "aclwrite"); err != nil {
		return nil
	}
	return errExpectFail
}

func listbucket(ctx context.Context, client *s3.Client, bucket string) error {
	input := s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	}
	_, err := client.ListObjectsV2(ctx, &input)
	return err
}
func testReadPermissionPass() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := setAcl(ctx, rootclient, tbuck, snil, "read"); err != nil {
		return err
	}
	user1client, err := getS3client(ctx, user1ak, user1sk)
	if err != nil {
		return err
	}
	if err := listbucket(ctx, user1client, tbuck); err != nil {
		return err
	}
	return nil
}

func testReadPermissionFail() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := setAcl(ctx, rootclient, tbuck, snil, "read"); err != nil {
		return err
	}
	user2client, err := getS3client(ctx, user2ak, user2sk)
	if err != nil {
		return err
	}
	if err := listbucket(ctx, user2client, tbuck); err != nil {
		return nil
	}
	return errExpectFail
}

func putobject(ctx context.Context, client *s3.Client, bucket, object string, reader io.Reader) error {
	input := s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
		Body:   reader,
	}

	_, err := client.PutObject(ctx, &input)
	return err
}

func testWritePermissionPass() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := setAcl(ctx, rootclient, tbuck, snil, "write"); err != nil {
		return err
	}
	user1client, err := getS3client(ctx, user1ak, user1sk)
	if err != nil {
		return err
	}
	if err := putobject(ctx, user1client, tbuck, tfile, strings.NewReader("this is test file content")); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, tfile)
	return nil
}

func testWritePermissionFail() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := setAcl(ctx, rootclient, tbuck, snil, "write"); err != nil {
		return err
	}
	user2client, err := getS3client(ctx, user2ak, user2sk)
	if err != nil {
		return err
	}
	if err := putobject(ctx, user2client, tbuck, tfile, strings.NewReader("this is test file content")); err != nil {
		return nil
	}
	defer finish(ctx, rootclient, tbuck, tfile)
	return errExpectFail
}

func testAclReadObjectPermissionPass() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := start(ctx, rootclient, tbuck, tfile, strings.NewReader("this is test file content")); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, tfile)
	if err := setAcl(ctx, rootclient, tbuck, tfile, "aclread"); err != nil {
		return err
	}
	user1client, err := getS3client(ctx, user1ak, user1sk)
	if err != nil {
		return err
	}
	if err := getAcl(ctx, user1client, tbuck, tfile); err != nil {
		return err
	}
	return nil
}

func testAclReadObjectPermissionFail() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := start(ctx, rootclient, tbuck, tfile, strings.NewReader("this is test file content")); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, tfile)
	if err := setAcl(ctx, rootclient, tbuck, tfile, "aclread"); err != nil {
		return err
	}
	user2client, err := getS3client(ctx, user2ak, user2sk)
	if err != nil {
		return err
	}
	if err := getAcl(ctx, user2client, tbuck, tfile); err != nil {
		return nil
	}
	return errExpectFail
}

func testAclWriteObjectPermissionPass() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := start(ctx, rootclient, tbuck, tfile, strings.NewReader("this is test file content")); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, tfile)
	if err := setAcl(ctx, rootclient, tbuck, tfile, "aclwrite"); err != nil {
		return err
	}
	user1client, err := getS3client(ctx, user1ak, user1sk)
	if err != nil {
		return err
	}
	if err := setAcl(ctx, user1client, tbuck, tfile, "aclwrite"); err != nil {
		return err
	}
	return nil
}

func testAclWriteObjectPermissionFail() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := start(ctx, rootclient, tbuck, tfile, strings.NewReader("this is test file content")); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, tfile)
	if err := setAcl(ctx, rootclient, tbuck, tfile, "aclwrite"); err != nil {
		return err
	}
	user2client, err := getS3client(ctx, user2ak, user2sk)
	if err != nil {
		return err
	}
	if err := setAcl(ctx, user2client, tbuck, tfile, "aclwrite"); err != nil {
		return nil
	}
	return errExpectFail
}

func getobject(ctx context.Context, client *s3.Client, bucket, object string) error {
	input := s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}

	_, err := client.GetObject(ctx, &input)
	return err
}
func testReadObjectPass() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := start(ctx, rootclient, tbuck, tfile, strings.NewReader("this is test file content")); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, tfile)
	if err := setAcl(ctx, rootclient, tbuck, tfile, "read"); err != nil {
		return err
	}
	user1client, err := getS3client(ctx, user1ak, user1sk)
	if err != nil {
		return err
	}
	if err := getobject(ctx, user1client, tbuck, tfile); err != nil {
		return err
	}
	return nil
}

func testReadObjectFail() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := start(ctx, rootclient, tbuck, tfile, strings.NewReader("this is test file content")); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, tfile)
	if err := setAcl(ctx, rootclient, tbuck, tfile, "read"); err != nil {
		return err
	}
	user2client, err := getS3client(ctx, user2ak, user2sk)
	if err != nil {
		return err
	}
	if err := getobject(ctx, user2client, tbuck, tfile); err != nil {
		return nil
	}
	return errExpectFail
}

func abortMultiPart(ctx context.Context, client *s3.Client, bucket, object, uploadId string) error {
	input := s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(object),
		UploadId: aws.String(uploadId),
	}
	_, err := client.AbortMultipartUpload(ctx, &input)
	return err
}

func newMultiPart(ctx context.Context, client *s3.Client, bucket, object string) (string, error) {
	input := s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}

	res, err := client.CreateMultipartUpload(ctx, &input)
	if err != nil {
		return "", err
	}

	return aws.ToString(res.UploadId), nil
}

var (
	tpart = "testpart"
)

func testNewMultiPartPass() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := setAcl(ctx, rootclient, tbuck, snil, "write"); err != nil {
		return err
	}
	user1client, err := getS3client(ctx, user1ak, user1sk)
	if err != nil {
		return err
	}
	id, err := newMultiPart(ctx, user1client, tbuck, tpart)
	if err != nil {
		return err
	}
	defer abortMultiPart(ctx, rootclient, tbuck, tpart, id)
	return nil
}

func testNewMultiPartFail() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := setAcl(ctx, rootclient, tbuck, snil, "write"); err != nil {
		return err
	}
	user2client, err := getS3client(ctx, user2ak, user2sk)
	if err != nil {
		return err
	}
	id, err := newMultiPart(ctx, user2client, tbuck, tpart)
	if err != nil {
		return nil
	}
	defer abortMultiPart(ctx, rootclient, tbuck, tpart, id)
	return errExpectFail
}

func uploadPart(ctx context.Context, client *s3.Client, bucket, object, uploadId string, partn int32, reader io.Reader) (string, error) {
	input := s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(object),
		UploadId:   aws.String(uploadId),
		PartNumber: partn,
		Body:       reader,
	}

	res, err := client.UploadPart(ctx, &input)
	if err != nil {
		return "", err
	}
	return aws.ToString(res.ETag), nil
}

func testUploadPartPass() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := setAcl(ctx, rootclient, tbuck, snil, "write"); err != nil {
		return err
	}
	user1client, err := getS3client(ctx, user1ak, user1sk)
	if err != nil {
		return err
	}
	id, err := newMultiPart(ctx, user1client, tbuck, tpart)
	if err != nil {
		return err
	}
	defer abortMultiPart(ctx, rootclient, tbuck, tpart, id)
	if err := createfile(tpart, 10*mb); err != nil {
		return err
	}
	defer remove(tpart)
	f, err := os.Open(tpart)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := uploadPart(ctx, user1client, tbuck, tpart, id, 1, f); err != nil {
		return err
	}
	return nil
}

func testUploadPartFail() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := setAcl(ctx, rootclient, tbuck, snil, "write"); err != nil {
		return err
	}
	user1client, err := getS3client(ctx, user1ak, user1sk)
	if err != nil {
		return err
	}
	id, err := newMultiPart(ctx, user1client, tbuck, tpart)
	if err != nil {
		return err
	}
	defer abortMultiPart(ctx, rootclient, tbuck, tpart, id)
	if err := createfile(tpart, 10*mb); err != nil {
		return err
	}
	defer remove(tpart)
	f, err := os.Open(tpart)
	if err != nil {
		return err
	}
	defer f.Close()
	user2client, err := getS3client(ctx, user2ak, user2sk)
	if err != nil {
		return err
	}
	if _, err := uploadPart(ctx, user2client, tbuck, tpart, id, 1, f); err != nil {
		return nil
	}
	return errExpectFail
}

type cpart struct {
	id   int32
	etag string
}

func completeMultipart(ctx context.Context, client *s3.Client, bucket, object, uploadId string, cparts ...cpart) error {
	parts := []types.CompletedPart{}
	for _, p := range cparts {
		cp := types.CompletedPart{
			PartNumber: p.id,
			ETag:       aws.String(p.etag),
		}
		parts = append(parts, cp)
	}
	input := s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(object),
		UploadId: aws.String(uploadId),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	}
	_, err := client.CompleteMultipartUpload(ctx, &input)
	return err
}

func testCompleteMultiPartPass() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := setAcl(ctx, rootclient, tbuck, snil, "write"); err != nil {
		return err
	}
	user1client, err := getS3client(ctx, user1ak, user1sk)
	if err != nil {
		return err
	}
	id, err := newMultiPart(ctx, user1client, tbuck, tpart)
	if err != nil {
		return err
	}
	if err := createfile(tpart, 10*mb); err != nil {
		return err
	}
	defer remove(tpart)
	f, err := os.Open(tpart)
	if err != nil {
		return err
	}
	defer f.Close()
	etag, err := uploadPart(ctx, user1client, tbuck, tpart, id, 1, f)
	if err != nil {
		return err
	}
	cp := cpart{
		id:   1,
		etag: etag,
	}
	if err := completeMultipart(ctx, user1client, tbuck, tpart, id, cp); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, tpart)
	return nil
}

func testCompleteMultiPartFail() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := setAcl(ctx, rootclient, tbuck, snil, "write"); err != nil {
		return err
	}
	user1client, err := getS3client(ctx, user1ak, user1sk)
	if err != nil {
		return err
	}
	id, err := newMultiPart(ctx, user1client, tbuck, tpart)
	if err != nil {
		return err
	}
	if err := createfile(tpart, 10*mb); err != nil {
		return err
	}
	defer remove(tpart)
	f, err := os.Open(tpart)
	if err != nil {
		return err
	}
	defer f.Close()
	etag, err := uploadPart(ctx, user1client, tbuck, tpart, id, 1, f)
	if err != nil {
		return err
	}
	cp := cpart{
		id:   1,
		etag: etag,
	}
	user2client, err := getS3client(ctx, user2ak, user2sk)
	if err != nil {
	}
	if err := completeMultipart(ctx, user2client, tbuck, tpart, id, cp); err != nil {
		defer abortMultiPart(ctx, rootclient, tbuck, tpart, id)
		return nil
	}
	defer finish(ctx, rootclient, tbuck, tpart)
	return errExpectFail
}

var (
	errMissId = errors.New("miss some uploadid in result")
)

func listMultipart(ctx context.Context, client *s3.Client, bucket string, ids ...string) error {
	input := s3.ListMultipartUploadsInput{
		Bucket: aws.String(bucket),
	}

	res, err := client.ListMultipartUploads(ctx, &input)
	if err != nil {
		return err
	}
	count := 0
	if len(ids) != 0 {
		for _, m := range res.Uploads {
			mid := aws.ToString(m.UploadId)
			for _, id := range ids {
				if id == mid {
					count++
				}
			}
		}
	}
	if count != len(ids) && len(ids) != 0 {
		return errMissId
	}

	return nil
}

var (
	tpart1 = "testpart1"
	tpart2 = "testpart2"
)

func testListMultiPartPass() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := setAcl(ctx, rootclient, tbuck, snil, "read"); err != nil {
		return err
	}
	id1, err := newMultiPart(ctx, rootclient, tbuck, tpart1)
	if err != nil {
		return err
	}
	defer abortMultiPart(ctx, rootclient, tbuck, tpart1, id1)
	id2, err := newMultiPart(ctx, rootclient, tbuck, tpart2)
	defer abortMultiPart(ctx, rootclient, tbuck, tpart2, id2)
	user1client, err := getS3client(ctx, user1ak, user1sk)
	if err != nil {
		return err
	}
	if err := listMultipart(ctx, user1client, tbuck, id1, id2); err != nil {
		return err
	}
	return nil
}

func testListMultiPartFail() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := setAcl(ctx, rootclient, tbuck, snil, "read"); err != nil {
		return err
	}
	id1, err := newMultiPart(ctx, rootclient, tbuck, tpart1)
	if err != nil {
		return err
	}
	defer abortMultiPart(ctx, rootclient, tbuck, tpart1, id1)
	id2, err := newMultiPart(ctx, rootclient, tbuck, tpart2)
	defer abortMultiPart(ctx, rootclient, tbuck, tpart2, id2)
	user2client, err := getS3client(ctx, user2ak, user2sk)
	if err != nil {
		return err
	}
	if err := listMultipart(ctx, user2client, tbuck, id1, id2); err != nil {
		return nil
	}
	return errExpectFail
}

func copyObject(ctx context.Context, client *s3.Client, srcbucket, srcobject, bucket, object string) error {
	input := s3.CopyObjectInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(object),
		CopySource: aws.String(srcbucket + "/" + srcobject),
	}

	_, err := client.CopyObject(ctx, &input)
	return err
}

var (
	cfile = "copyfile"
)

func testCopyObjectPass() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := start(ctx, rootclient, tbuck, tfile, strings.NewReader("this is test file content")); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, tfile)
	if err := setAcl(ctx, rootclient, tbuck, snil, "write"); err != nil {
		return err
	}
	if err := setAcl(ctx, rootclient, tbuck, tfile, "read"); err != nil {
		return err
	}
	user1client, err := getS3client(ctx, user1ak, user1sk)
	if err != nil {
		return err
	}
	if err := copyObject(ctx, user1client, tbuck, tfile, tbuck, cfile); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, cfile)
	return nil
}

func testCopyObjectFail() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := start(ctx, rootclient, tbuck, tfile, strings.NewReader("this is test file content")); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, tfile)
	if err := setAcl(ctx, rootclient, tbuck, tfile, "read"); err != nil {
		return err
	}
	user1client, err := getS3client(ctx, user1ak, user1sk)
	if err != nil {
		return err
	}
	if err := copyObject(ctx, user1client, tbuck, tfile, tbuck, cfile); err != nil {
		return nil
	}
	defer finish(ctx, rootclient, tbuck, cfile)
	return errExpectFail
}

func tesetCopyObjectReadFail() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := start(ctx, rootclient, tbuck, tfile, strings.NewReader("this is test file content")); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, tfile)
	if err := setAcl(ctx, rootclient, tbuck, snil, "write"); err != nil {
		return err
	}
	user1client, err := getS3client(ctx, user1ak, user1sk)
	if err != nil {
		return err
	}
	if err := copyObject(ctx, user1client, tbuck, tfile, tbuck, cfile); err != nil {
		return nil
	}
	defer finish(ctx, rootclient, tbuck, cfile)
	return errExpectFail
}

func copyUploadPart(ctx context.Context, client *s3.Client, srcbucket, srcobject,
	bucket, object, uploadId string, partn int32) error {
	input := s3.UploadPartCopyInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(object),
		CopySource: aws.String(srcbucket + "/" + srcobject),
		UploadId:   aws.String(uploadId),
		PartNumber: partn,
	}
	_, err := client.UploadPartCopy(ctx, &input)
	return err
}

func testUploadCopyPass() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := createfile(tpart, 10*mb); err != nil {
		return err
	}
	defer remove(tpart)
	f, err := os.Open(tpart)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := start(ctx, rootclient, tbuck, tfile, f); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, tfile)
	if err := setAcl(ctx, rootclient, tbuck, snil, "write"); err != nil {
		return err
	}
	if err := setAcl(ctx, rootclient, tbuck, tfile, "read"); err != nil {
		return err
	}
	user1client, err := getS3client(ctx, user1ak, user1sk)
	if err != nil {
		return err
	}
	id, err := newMultiPart(ctx, user1client, tbuck, tpart)
	if err != nil {
		return err
	}
	defer abortMultiPart(ctx, rootclient, tbuck, tpart, id)
	if err := copyUploadPart(ctx, user1client, tbuck, tfile, tbuck, tpart, id, 1); err != nil {
		return err
	}
	return nil
}

func testUploadCopyFail() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := createfile(tpart, 10*mb); err != nil {
		return err
	}
	defer remove(tpart)
	f, err := os.Open(tpart)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, tfile, f); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, tfile)
	if err := setAcl(ctx, rootclient, tbuck, snil, "write"); err != nil {
		return err
	}
	user1client, err := getS3client(ctx, user1ak, user1sk)
	if err != nil {
		return err
	}
	id, err := newMultiPart(ctx, user1client, tbuck, tpart)
	if err != nil {
		return err
	}
	defer abortMultiPart(ctx, rootclient, tbuck, tpart, id)
	if err := copyUploadPart(ctx, user1client, tbuck, tfile, tbuck, tpart, id, 1); err != nil {
		return nil
	}
	return errExpectFail
}

var (
	selectinput = `{"id": 0,"title": "Test Record","desc": "Some text","synonyms": ["foo", "bar", "whatever"]}
        {"id": 1,"title": "Second Record","desc": "another text","synonyms": ["some", "synonym", "value"]}
        {"id": 2,"title": "Second Record","desc": "another text","numbers": [2, 3.0, 4]}
        {"id": 3,"title": "Second Record","desc": "another text","nested": [[2, 3.0, 4], [7, 8.5, 9]]}`
	sqlinput = `SELECT * from s3object s WHERE 'bar' IN s.synonyms[*]`
)

func selectContent(ctx context.Context, client *s3.Client, bucket, object, exp string) error {
	input := s3.SelectObjectContentInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(object),
		Expression: aws.String(exp),
		InputSerialization: &types.InputSerialization{
			JSON: &types.JSONInput{
				Type: types.JSONTypeLines,
			},
		},
		OutputSerialization: &types.OutputSerialization{
			JSON: &types.JSONOutput{},
		},
		ExpressionType:types.ExpressionTypeSql,
	}
	_, err := client.SelectObjectContent(ctx, &input)
	return err
}

func testSelectContentPass() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := start(ctx, rootclient, tbuck, tfile, strings.NewReader(selectinput)); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, tfile)
	if err := setAcl(ctx, rootclient, tbuck, tfile, "read"); err != nil {
		return err
	}
	user1client, err := getS3client(ctx, user1ak, user1sk)
	if err != nil {
		return err
	}
	if err := selectContent(ctx, user1client, tbuck, tfile, sqlinput); err != nil {
		return err
	}
	return nil
}

func testSelectContentFail() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := start(ctx, rootclient, tbuck, tfile, strings.NewReader(selectinput)); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, tfile)
	if err := setAcl(ctx, rootclient, tbuck, tfile, "read"); err != nil {
		return err
	}
	user2client, err := getS3client(ctx, user2ak, user2sk)
	if err != nil {
		return err
	}
	if err := selectContent(ctx, user2client, tbuck, tfile, sqlinput); err != nil {
		return nil
	}
	return errExpectFail
}

func headop(ctx context.Context, client *s3.Client, bucket, object string) error {
	if object == "" {
		input := s3.HeadBucketInput{
			Bucket: aws.String(bucket),
		}
		_, err := client.HeadBucket(ctx, &input)
		return err
	}
	input := s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}
	_, err := client.HeadObject(ctx, &input)
	return err
}

func testheadBucketPass() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := setAcl(ctx, rootclient, tbuck, snil, "read"); err != nil {
		return err
	}
	user1client, err := getS3client(ctx, user1ak, user1sk)
	if err != nil {
		return err
	}
	if err := headop(ctx, user1client, tbuck, snil); err != nil {
		return err
	}
	return nil
}

func testheadBucketFail() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := setAcl(ctx, rootclient, tbuck, snil, "read"); err != nil {
		return err
	}
	user2client, err := getS3client(ctx, user2ak, user2sk)
	if err != nil {
		return err
	}
	if err := headop(ctx, user2client, tbuck, snil); err != nil {
		return nil
	}
	return errExpectFail
}

func testheadObjectPass() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := start(ctx, rootclient, tbuck, tfile, strings.NewReader("this is test file content")); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, tfile)
	if err := setAcl(ctx, rootclient, tbuck, tfile, "read"); err != nil {
		return err
	}
	user1client, err := getS3client(ctx, user1ak, user1sk)
	if err != nil {
		return err
	}
	if err := headop(ctx, user1client, tbuck, tfile); err != nil {
		return err
	}
	return nil
}

func testheadObjectFail() error {
	ctx := context.Background()
	rootclient, err := getS3client(ctx, rootak, rootsk)
	if err != nil {
		return err
	}
	if err := start(ctx, rootclient, tbuck, snil, nil); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, snil)
	if err := start(ctx, rootclient, tbuck, tfile, strings.NewReader("this is test file content")); err != nil {
		return err
	}
	defer finish(ctx, rootclient, tbuck, tfile)
	if err := setAcl(ctx, rootclient, tbuck, tfile, "read"); err != nil {
		return err
	}
	user2client, err := getS3client(ctx, user2ak, user2sk)
	if err != nil {
		return err
	}
	if err := headop(ctx, user2client, tbuck, tfile); err != nil {
		return nil
	}
	return errExpectFail
}

var testcases = map[string]func() error{
	"testAclReadPermissionPass":        testAclReadPermissionPass,
	"testAclReadPermissionFail":        testAclReadPermissionFail,
	"testAclWritePermissionPass":       testAclWritePermissionPass,
	"testAclWritePermissionFail":       testAclWritePermissionFail,
	"testReadPermissionPass":           testReadPermissionPass,
	"testReadPermissionFail":           testReadPermissionFail,
	"testWritePermissionPass":          testWritePermissionPass,
	"testWritePermissionFail":          testWritePermissionFail,
	"testAclReadObjectPermissionPass":  testAclReadObjectPermissionPass,
	"testAclReadObjectPermissionFail":  testAclReadObjectPermissionFail,
	"testAclWriteObjectPermissionPass": testAclWriteObjectPermissionPass,
	"testAclWriteObjectPermissionFail": testAclWriteObjectPermissionFail,
	"testReadObjectPass":               testReadObjectPass,
	"testReadObjectFail":               testReadObjectFail,
	"testNewMultiPartPass":             testNewMultiPartPass,
	"testNewMultiPartFail":             testNewMultiPartFail,
	"testUploadPartPass":               testUploadPartPass,
	"testUploadPartFail":               testUploadPartFail,
	"testCompleteMultiPartPass":        testCompleteMultiPartPass,
	"testCompleteMultiPartFail":        testCompleteMultiPartFail,
	"testListMultiPartPass":            testListMultiPartPass,
	"testListMultiPartFail":            testListMultiPartFail,
	"testCopyObjectPass":               testCopyObjectPass,
	"testCopyObjectFail":               testCopyObjectFail,
	"testUploadCopyPass":               testUploadCopyPass,
	"testUploadCopyFail":               testUploadCopyFail,
	"testSelectContentPass":            testSelectContentPass,
	"testSelectContentFail":            testSelectContentFail,
	"testheadBucketPass":               testheadBucketPass,
	"testheadBucketFail":               testheadBucketFail,
	"testheadObjectPass":               testheadObjectPass,
	"testheadObjectFail":               testheadObjectFail,
}

func main() {
	for name, f := range testcases {
		if err := f(); err != nil {
			fmt.Printf("%35v fail\n", name)
			//continue
			break
		}
		fmt.Printf("%35v pass\n", name)
	}
}
