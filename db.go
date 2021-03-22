package db

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Configuration Options to database.
type mongoDB struct {
	Host string
	Port int16
	User string
	password string
	AuthMechanism string
	Service bool
	clientOptions *options.ClientOptions
	client *mongo.Client
	selectedDb string
}

func (opt *mongoDB) String() string {
	var userSegment string
	if len(opt.User) > 0 && len(opt.password) > 0 {
		userSegment = fmt.Sprintf("%s:%s@", opt.User, opt.password)
	}
	var serviceDescription string = "mongodb"
	if opt.Service {
		serviceDescription = "mongodb+srv"
	}
	return fmt.Sprintf("%s://%s%s:%d",serviceDescription, userSegment, opt.Host, opt.Port)
}

func (conf *mongoDB) Connect() error {
	var err error
	conf.client, err = mongo.Connect(context.TODO(), conf.clientOptions)
	if err != nil {
		return err
	}

	err = conf.client.Ping(context.TODO(), nil)
	if err != nil {
		return err
	}

	return nil
}

// Config mongo with host and default port 27017
func Config(host string, dboptions ...func (*mongoDB)) *mongoDB {
	conf := mongoDB {
		Host: host,
		Port: 27017,
	}

	for _, option := range dboptions {
		option(&conf)
	}

	conf.clientOptions = options.Client().ApplyURI(conf.String())
	conf.setAuth()
	return &conf
}

func (conf *mongoDB) setAuth() {
	cred := options.Credential { }
	if len(conf.User) > 0 && len(conf.password) > 0 {
		cred = options.Credential {
			Username: conf.User,
			Password: conf.password,
			AuthMechanism: conf.AuthMechanism,
		}
		conf.clientOptions = conf.clientOptions.SetAuth(cred)
	}
}

func (conf *mongoDB) SetPort(port int16) func(*mongoDB) {
	return func(c *mongoDB) {
		c.Port = int16(port)
	}
}

func Auth(user string, password string, authMechanism string) func(*mongoDB)  {
	return func(o *mongoDB)  {
		o.User = user
		o.password = password
		o.AuthMechanism = authMechanism
	}
}

func IsService(is bool) func(*mongoDB) {
	return func(o *mongoDB) {
		o.Service = true
	}
}

func (conf *mongoDB) SelectDB(db string) {
	conf.selectedDb = db
}

func (conf *mongoDB) Insert(records []interface{}, collectionName string) (*mongo.InsertManyResult, error){
	var err error
	var result *mongo.InsertManyResult
	collection := conf.client.Database(conf.selectedDb).Collection(collectionName)
	if len(records) > 0 {
		result, err = collection.InsertMany(context.TODO(), records)
	}
	return result, err
}