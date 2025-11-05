package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// https://medium.com/@sharmavivek1709/building-a-scalable-object-storage-solution-with-golang-and-minio-b0080c4e41db

type Storage struct {
	client     *minio.Client
	BucketName string
}

func NewStorage(endpoint, accessKeyID, secretAccessKey string, useSSL bool) *Storage {
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Fatalln(err)
	}
	return &Storage{
		client: minioClient,
		// BucketName: bucketName,
	}
}

func (s *Storage) CreateBucketWithCheck(ctx context.Context, bucketName string) error {
	location := "us-east-1"
	err := s.client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: location})
	if err != nil {
		exists, errBucketExists := s.client.BucketExists(context.Background(), bucketName)
		if errBucketExists == nil && exists {
			return errors.New("Bucket already exists")
		} else {
			return err
		}
	}
	fmt.Println("Bucket created successfully!")
	return nil
}

func (s *Storage) PutObject(ctx context.Context, filepath, bucketname, objectName, contentType string) error {
	file, err := os.Open(filepath)
	if err != nil {
		return err
	}

	defer file.Close()

	fileStat, err := file.Stat()
	if err != nil {
		return err
	}
	uploadInfo, err := s.client.PutObject(ctx, bucketname, objectName, file, fileStat.Size(),
		minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		return err
	}
	fmt.Println("Info upload: ", uploadInfo)
	return nil
}

func (s *Storage) ListObjects(ctx context.Context, bucketName string) error {
	objectCh := s.client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		// Prefix:    "myprefix",
		Recursive: true,
	})
	for object := range objectCh {
		if object.Err != nil {
			fmt.Println("error while streaming the response from the object: ", object.Err)
			return object.Err
		}
		fmt.Println(object)
	}
	return nil
}

func main() {
	endpoint := "localhost:9000"
	accessKeyID := "minioadmin"
	secretAccessKey := "minioadmin"
	useSSL := false
	ctx := context.Background()

	// Initialize minio client object.
	bucketName := "firstbucket"
	storage := NewStorage(endpoint, accessKeyID, secretAccessKey, useSSL)

	// CREATE BUCKET
	// err := storage.CreateBucketWithCheck(ctx, "firstbucket")

	// UPLOAD OBJECT
	dir, _ := os.Getwd()
	fp := filepath.Join(dir, "template_berkas_organisasi.xlsx")
	err := storage.PutObject(ctx, fp, bucketName, "newfile", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	if err != nil {
		log.Fatal(err)
	}

	// LIST OBJECT
	err = storage.ListObjects(ctx, bucketName)
	if err != nil {
		log.Fatal(err)
	}
}
