package mongodb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Client struct {
	client   *mongo.Client
	database *mongo.Database
}

type Config struct {
	URI      string
	Database string
}

func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	clientOpts := options.Client().ApplyURI(cfg.URI)

	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return nil, err
	}

	if err := client.Ping(connectCtx, nil); err != nil {
		return nil, err
	}

	return &Client{
		client:   client,
		database: client.Database(cfg.Database),
	}, nil
}

func (c *Client) Collection(name string) *mongo.Collection {
	return c.database.Collection(name)
}

func (c *Client) Database() *mongo.Database {
	return c.database
}

func (c *Client) Close(ctx context.Context) error {
	return c.client.Disconnect(ctx)
}
