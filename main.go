package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/xuri/excelize/v2"
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

// PutObject
//
// ObjectName can contains path too: i.e. /path1/path2/readme.txt
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

func (s *Storage) GetObject(ctx context.Context, bucketName, objectName string) error {
	obj, err := s.client.GetObject(ctx, bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		return err
	}

	defer obj.Close()

	localFile, err := os.Create("/tmp/download.xlsx")
	if err != nil {
		return err
	}
	defer localFile.Close()

	if _, err := io.Copy(localFile, obj); err != nil {
		return err
	}
	fmt.Println("download success")
	return nil
}

// ListObjects
//
// Prefix can be filepath or filename
func (s *Storage) ListObjects(ctx context.Context, bucketName string, prefix string) error {
	objectCh := s.client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		Prefix:    prefix,
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

// FileExists
//
// ObjectName must be exact path and filename
func (s *Storage) FileExists(ctx context.Context, bucketName, objectName string) (bool, error) {
	_, err := s.client.StatObject(ctx, bucketName, objectName, minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return false, nil // File not found
		}
		return false, err // Other error
	}
	return true, nil
}

func (s *Storage) GetPresignedUrl(ctx context.Context, bucket, object string) (string, error) {
	reqParams := make(url.Values)
	u, err := s.client.PresignedGetObject(ctx, bucket, object, 3*time.Minute, reqParams)
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

func readExcel() {
	// os.Open()
	filename, _ := os.Getwd()
	filename = filepath.Join(filename, "template_berkas_organisasi.xlsx")
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	f, err := excelize.OpenFile(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	rows, err := f.GetRows(f.GetSheetName(0))
	if err != nil {
		log.Fatal(err)
	}
	// fmt.Println(rows, len(rows))
	for i, cols := range rows {
		fmt.Print(len(cols), " => ", i)
		for _, val := range cols {
			fmt.Print(val, ",")
		}
		fmt.Println()
	}
}

func main() {
	var err error

	// Load .env file
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using system environment variables")
	}

	// Read values
	endpoint := os.Getenv("MINIO_ENDPOINT")
	accessKeyID := os.Getenv("MINIO_ACCESS_KEY")
	secretAccessKey := os.Getenv("MINIO_SECRET_KEY")

	// Parse boolean from string
	useSSL, err := strconv.ParseBool(os.Getenv("MINIO_USE_SSL"))
	if err != nil {
		useSSL = false // default fallback
	}
	ctx := context.Background()

	// Initialize minio client object.
	bucketName := "palm-attendance"
	storage := NewStorage(endpoint, accessKeyID, secretAccessKey, useSSL)
	// CREATE BUCKET
	// err = storage.CreateBucketWithCheck(ctx, bucketName)

	// UPLOAD OBJECT
	// dir, _ := os.Getwd()
	// fp := filepath.Join(dir, "template_berkas_organisasi.xlsx")
	// err = storage.PutObject(ctx, fp, bucketName, "temp/template_berkas_organisasi.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	// if err != nil {
	// log.Fatal(err)
	// }

	// LIST OBJECT
	err = storage.ListObjects(ctx, bucketName, "public/excel_template/template_berkas_organisasi")
	if err != nil {
		log.Fatal(err)
	}

	// GET/DOWNLOAD OBJECT
	// err = storage.GetObject(ctx, bucketName, "newfile")
	// if err != nil {
	// log.Fatal(err)
	// }

	// CHECK FILE EXIST OR NOT
	ok, err := storage.FileExists(ctx, bucketName, "public/excel_template/template_berkas_organisasi.xlsx")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("is exist? :", ok)

	// GET URL FOR DOWNLOAD
	url, err := storage.GetPresignedUrl(ctx, bucketName, "public/excel_template/template_berkas_organisasi.xlsx")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("URL: ", url)
}
