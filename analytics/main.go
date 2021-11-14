package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/riferrei/srclient"
	"google.golang.org/protobuf/proto"

	pc "schemas"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-session/session"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

const (
	authServerURL = "http://localhost:9096"
)

type SchemaRegistryClient interface {
	GetSchema(schemaID int) (*srclient.Schema, error)
	GetLatestSchema(subject string) (*srclient.Schema, error)
	CreateSchema(subject string, schema string, schemaType srclient.SchemaType, references ...srclient.Reference) (*srclient.Schema, error)
	IsSchemaCompatible(subject, schema, version string, schemaType srclient.SchemaType) (bool, error)
}

func getCurrentUser(w http.ResponseWriter, r *http.Request, conn *pgx.Conn) (User, error) {
	var user User
	store, err := session.Start(r.Context(), w, r)
	if err != nil {
		return user, err
	}

	var checkID int64
	if v, ok := store.Get("user_id"); ok {
		checkID, _ = strconv.ParseInt(v.(string), 10, 64)
	} else {
		return user, err
	}

	err = conn.QueryRow(context.Background(), "select id,username,role from account_users where id=$1", checkID).Scan(&user.ID, &user.Username, &user.Role)
	if err != nil {
		return user, err
	}
	return user, nil
}

type ProtobufDeserializer struct {
	client      SchemaRegistryClient
	topic       string
	valueSchema *srclient.Schema
}

func NewProtobufDeserializer(schemaRegistryClient SchemaRegistryClient, topic string) *ProtobufDeserializer {

	valueSchema, err := schemaRegistryClient.GetLatestSchema(topic)
	if err != nil {
		panic(fmt.Sprintf("Error fetching the value schema %s", err))
	}

	return &ProtobufDeserializer{
		client:      schemaRegistryClient,
		topic:       topic,
		valueSchema: valueSchema,
	}
}

func (ps *ProtobufDeserializer) Deserialize(bytes []byte, pb proto.Message) error {
	// decode the number of elements in the array of message indexes
	arrayLen, bytesRead := binary.Varint(bytes[5:])
	if bytesRead <= 0 {
		err := fmt.Errorf("Unable to decode message index array")
		return err
	}
	totalBytesRead := bytesRead
	msgIndexArray := make([]int64, arrayLen)
	// iterate arrayLen times, decoding another varint
	for i := int64(0); i < arrayLen; i++ {
		idx, bytesRead := binary.Varint(bytes[5+totalBytesRead:])
		if bytesRead <= 0 {
			err := fmt.Errorf("Unable to decode value in message index array")
			return err
		}
		totalBytesRead += bytesRead
		msgIndexArray[i] = idx
	}
	err := proto.Unmarshal(bytes[5+totalBytesRead:], pb)
	if err != nil {
		fmt.Printf("failed deserialize: %v", err)
		return err
	}
	return nil
}
func GetAssignPrice() int {
	rand.Seed(time.Now().UnixNano())
	min := -10
	max := -20
	return rand.Intn(max-min+1) + min
}
func GetFinishedPrice() int {
	rand.Seed(time.Now().UnixNano())
	min := 20
	max := 40
	return rand.Intn(max-min+1) + min
}
func (ps *ProtobufDeserializer) GetTopic() *string {
	return &ps.topic
}

func calculateEventsCost(events []AuditLog) int64 {
	var total int64
	for _, event := range events {
		switch action := event.Action; action {
		case "assign":
			total = total - event.Cost
		case "finished":
			total = total + event.Cost
		case "pay":
			total = 0

		}
	}
	return total
}

var (
	config = oauth2.Config{
		ClientID:     "analytics-service",
		ClientSecret: "22222222",
		Scopes:       []string{"read", "write", "update", "delete"},
		RedirectURL:  "http://localhost:9096/oauth2",
		Endpoint: oauth2.Endpoint{
			AuthURL:  authServerURL + "/oauth/authorize",
			TokenURL: authServerURL + "/oauth/token",
		},
	}
	globalToken *oauth2.Token // Non-concurrent security
)

type ResponseOauth struct {
	UserId string `json:"user_id"`
}

type User struct {
	ID       int64
	PublicID string
	Username string
	Role     string
}

func main() {
	urlExample := "postgres://user:passwd@localhost:5432/user"
	conn, err := pgx.Connect(context.Background(), urlExample)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://localhost:8081")
	c.SubscribeTopics([]string{"dc1.accounting.cdc.assigned", "dc1.accounting.cdc.finished"}, nil)

	deserializer := NewProtobufDeserializer(schemaRegistryClient, "dc1.task.cdc.tasks")

	go func() {
		for {
			msg, err := c.ReadMessage(-1)
			if err == nil {
				fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
				task := pc.Task{}
				deserializer.Deserialize(msg.Value, &task)
				switch eventName := task.Header.EventDescriptor; eventName {
				case "task.assigned":
					_ = conn.QueryRow(context.Background(), "insert analytic_log(task_id, user_id, action, created_at, cost) values($1,$2,$3,$4,$5)", task.Publicid, task.Userid, "assigned", task.CreatedAt, task.Cost)
				case "task.finished":
					_ = conn.QueryRow(context.Background(), "insert audit_log(task_id, user_id, action, created_at, cost) values($1,$2,$3,$4,$5)", task.Publicid, task.Userid, "finished", task.CreatedAt, task.Cost)
				}
			} else {
				// The client will automatically try to recover from all errors.
				log.Printf("Consumer error: %v (%v)\n", err, msg)
			}
		}

		c.Close()
	}()

	c.SubscribeTopics([]string{"dc1.task.cdc.users"}, nil)
	go func() {
		for {
			msg, err := c.ReadMessage(-1)
			if err == nil {
				fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
				user := pc.User{}
				deserializer.Deserialize(msg.Value, &user)
				switch eventName := user.Header.EventDescriptor; eventName {
				case "user.created":
					var id int64
					_ = conn.QueryRow(context.Background(), "insert into users(public_id, username, role) values($1,$2,$3) RETURNING id;", user.Publicid, user.Username, user.Role).Scan(&id)
				case "user.updated":
					_ = conn.QueryRow(context.Background(), "update users set username = $1, role = $2 where public_id = $3", user.Username, user.Role, user.Publicid)
				}
			} else {
				// The client will automatically try to recover from all errors.
				log.Printf("Consumer error: %v (%v)\n", err, msg)
			}
		}

		c.Close()
	}()

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		u := config.AuthCodeURL("xyz",
			oauth2.SetAuthURLParam("code_challenge", genCodeChallengeS256("s256example")),
			oauth2.SetAuthURLParam("code_challenge_method", "S256"))
		http.Redirect(w, r, u, http.StatusFound)
	})

	http.HandleFunc("/oauth2", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		state := r.Form.Get("state")
		if state != "xyz" {
			http.Error(w, "State invalid", http.StatusBadRequest)
			return
		}
		code := r.Form.Get("code")
		if code == "" {
			http.Error(w, "Code not found", http.StatusBadRequest)
			return
		}
		token, err := config.Exchange(context.Background(), code, oauth2.SetAuthURLParam("code_verifier", "s256example"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		globalToken = token

		resp, err := http.Get(fmt.Sprintf("%s/test?access_token=%s", authServerURL, globalToken.AccessToken))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)

		var result ResponseOauth
		if err := json.Unmarshal(body, &result); err != nil { // Parse []byte to go struct pointer
			fmt.Println("Can not unmarshal JSON")
		}

		store, err := session.Start(nil, w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		store.Set("user_id", result.UserId)
		store.Save()
		fmt.Println("store save")
		w.Header().Set("Location", "/dashboard")
		w.WriteHeader(http.StatusFound)
		return
	})

	http.HandleFunc("/refresh", func(w http.ResponseWriter, r *http.Request) {
		if globalToken == nil {
			http.Redirect(w, r, "/", http.StatusFound)
			return
		}

		globalToken.Expiry = time.Now()
		token, err := config.TokenSource(context.Background(), globalToken).Token()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		globalToken = token
		e := json.NewEncoder(w)
		e.SetIndent("", "  ")
		e.Encode(token)
	})

	http.HandleFunc("/try", func(w http.ResponseWriter, r *http.Request) {
		if globalToken == nil {
			http.Redirect(w, r, "/", http.StatusFound)
			return
		}

		resp, err := http.Get(fmt.Sprintf("%s/test?access_token=%s", authServerURL, globalToken.AccessToken))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer resp.Body.Close()

		io.Copy(w, resp.Body)
	})

	http.HandleFunc("/dashboard", func(w http.ResponseWriter, r *http.Request) {
		type Data struct {
			BadPopugsCount int64
			MaxPayedCost   int64
			CurrentUser    User
		}

		currentUser, err := getCurrentUser(w, r, conn)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if currentUser.Role != "admin" {
			return
		}
		var data Data
		data.CurrentUser = currentUser
		now := time.Now()
		from := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).Format("2006-01-02T15:04:05.000Z")
		to := time.Date(now.Year(), now.Month(), now.Day(), 23, 59, 0, 0, time.UTC).Format("2006-01-02T15:04:05.000Z")

		var users []User
		var auditLogs []AuditLog
		var rows pgx.Rows
		rows, err = conn.Query(context.Background(), "select id from users")

		for rows.Next() {
			var user User
			err := rows.Scan(&user.ID)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			users = append(users, user)
		}

		var badPopugs []Users
		for _, u := range users {
			rows, err = conn.Query(context.Background(), "select id, task_id, user_id, action, created_at, cost from analytic_log where created_at >= $1 and created_at <= $2", from, to, u.ID)
			if err != nil {
				log.Println("sql err", err)
			}
			for rows.Next() {
				var auditLog AuditLog
				err := rows.Scan(&auditLog.ID, &auditLog.TaskId, &auditLog.UserId, &auditLog.Action, &auditLog.CreatedAt, &auditLog.Cost)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				auditLogs = append(auditLogs, auditLog)
			}
			total := calculateEventsCost(auditLogs)
			if total < 0 {
				badPopugs = append(badPopugs, u)
			}
		}

		data.BadPopugsCount = len(badPopugs)

		var max int64
		_ = conn.QueryRow(context.Background(), "select max(cost) from analytic_log where created_at >= $1 and created_at <= $2 and action = 'finished'", from, to).Scan(&max)
		data.MaxPayedCost = max

		rows.Close()

		tmpl, err := template.ParseFiles("static/list.html")
		tmpl.Execute(w, data)
	})

	http.HandleFunc("/client", func(w http.ResponseWriter, r *http.Request) {
		cfg := clientcredentials.Config{
			ClientID:     config.ClientID,
			ClientSecret: config.ClientSecret,
			TokenURL:     config.Endpoint.TokenURL,
		}

		token, err := cfg.Token(context.Background())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		e := json.NewEncoder(w)
		e.SetIndent("", "  ")
		e.Encode(token)
	})

	log.Println("Client is running at 9098 port.Please open http://localhost:9098")
	log.Fatal(http.ListenAndServe(":9098", nil))
}

func genCodeChallengeS256(s string) string {
	s256 := sha256.Sum256([]byte(s))
	return base64.URLEncoding.EncodeToString(s256[:])
}
