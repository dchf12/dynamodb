package main

import (
	"context"
	"log"

	dynamodblocal "github.com/abhirockzz/dynamodb-local-testcontainers-go"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	tableName    = "Books"
	pkColumnName = "ISBN"
)

func main() {

	ctx := context.Background()

	dynamodbLocalContainer, err := dynamodblocal.RunContainer(ctx)
	if err != nil {
		log.Fatalf("failed to start container: %s", err)
	}

	// Clean up the container
	defer func() {
		if err := dynamodbLocalContainer.Terminate(ctx); err != nil {
			log.Fatalf("failed to terminate container: %s", err)
		}
	}()

	client, err := dynamodbLocalContainer.GetDynamoDBClient(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	err = createTable(client)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("dynamodb table created")

	result, err := client.ListTables(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}

	actualTableName := result.TableNames[0]
	log.Println("table", actualTableName, "found")

	value := "11111111111111111"
	err = addDataToTable(client, value)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("added data to dynamodb table")

	queryResult, err := queryItem(client, value)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("queried data from dynamodb table. result -", queryResult)
}

func createTable(client *dynamodb.Client) error {
	_, err := client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(pkColumnName),
				KeyType:       types.KeyTypeHash,
			},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(pkColumnName),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})

	if err != nil {
		return err
	}

	//log.Println("created table")
	return nil
}

func addDataToTable(client *dynamodb.Client, val string) error {

	_, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			pkColumnName: &types.AttributeValueMemberS{Value: val},
		},
	})

	if err != nil {
		return err
	}

	return nil
}

func queryItem(client *dynamodb.Client, val string) (string, error) {

	output, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			pkColumnName: &types.AttributeValueMemberS{Value: val},
		},
	})

	if err != nil {
		return "", err
	}

	result := output.Item[pkColumnName].(*types.AttributeValueMemberS)

	return result.Value, nil
}
