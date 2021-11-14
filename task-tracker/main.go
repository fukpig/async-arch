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

	pc "schemas"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-session/session"
	"github.com/jackc/pgx/v4"
	"github.com/riferrei/srclient"
	uuid "github.com/satori/go.uuid"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"google.golang.org/protobuf/proto"
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

type ProtobufSerializer struct {
	client        SchemaRegistryClient
	topic         string
	valueSchema   *srclient.Schema
	schemaIDBytes []byte
	msgIndexBytes []byte
}

func NewProtobufSerializer(schemaRegistryClient SchemaRegistryClient, topic string) *ProtobufSerializer {

	valueSchema, err := schemaRegistryClient.GetLatestSchema(topic)
	if err != nil {
		panic(fmt.Sprintf("Error fetching the value schema %s", err))
	}
	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(valueSchema.ID()))

	// 10 bytes is sufficient for 64 bits in zigzag encoding, but array indexes cannot possibly be
	// 64 bits in a reasonable protobuf, so let's just make a buffer sufficient for a reasonable
	// array of 4 or 5 elements, each with relatively small index
	varBytes := make([]byte, 16)
	// array length 1
	length := binary.PutVarint(varBytes, 1)
	// index 0 element.  We could write array length 0 with no subsequent value, which is equivalent to writing 1, 0
	length += binary.PutVarint(varBytes[length:], 0)

	return &ProtobufSerializer{
		client:        schemaRegistryClient,
		topic:         topic,
		valueSchema:   valueSchema,
		schemaIDBytes: schemaIDBytes,
		msgIndexBytes: varBytes[:length],
	}
}

func (ps *ProtobufSerializer) Serialize(pb proto.Message) ([]byte, error) {
	bytes, err := proto.Marshal(pb)
	if err != nil {
		fmt.Printf("failed serialize: %v", err)
		return nil, err
	}

	var msgBytes []byte
	// schema serialization protocol version number
	msgBytes = append(msgBytes, byte(0))
	// schema id
	msgBytes = append(msgBytes, ps.schemaIDBytes...)
	// zig zag encoded array of message indexes preceded by length of array
	msgBytes = append(msgBytes, ps.msgIndexBytes...)

	fmt.Printf("msgBytes is of length %d before proto\n", len(msgBytes))
	msgBytes = append(msgBytes, bytes...)

	return msgBytes, nil
}

func (ps *ProtobufSerializer) GetTopic() *string {
	return &ps.topic
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

	err = conn.QueryRow(context.Background(), "select id,username,role from tasks_users where id=$1", checkID).Scan(&user.ID, &user.Username, &user.Role)
	if err != nil {
		return user, err
	}
	return user, nil
}

var (
	config = oauth2.Config{
		ClientID:     "task-tracker",
		ClientSecret: "22222222",
		Scopes:       []string{"read", "write", "update", "delete"},
		RedirectURL:  "http://localhost:9094/oauth2",
		Endpoint: oauth2.Endpoint{
			AuthURL:  authServerURL + "/oauth/authorize",
			TokenURL: authServerURL + "/oauth/token",
		},
	}
	globalToken *oauth2.Token // Non-concurrent security

	hashKey = []byte("FF51A553-72FC-478B-9AEF-93D6F506DE91")
)

type ResponseOauth struct {
	UserId string `json:"user_id"`
}

type User struct {
	ID       int64
	Username string
	Role     string
}

type Task struct {
	ID          int64
	Description string
	UserID      int64
	AssignName  string
	Status      string
	CreatedAt   time.Time
}

type UserEventData struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
	Role     string `json:"role"`
}

type UserEvent struct {
	EventName string        `json:"event_name"`
	Data      UserEventData `json:"data"`
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

	//topic := "dc1.identity.cdc.users"
	topic := "dc1.task.cdc.tasks"
	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://localhost:8081")
	schema, err := schemaRegistryClient.GetLatestSchema(topic)
	if schema == nil {
		schemaBytes, _ := ioutil.ReadFile("schemas/task.v2.proto")
		schema, err = schemaRegistryClient.CreateSchema(topic, string(schemaBytes), srclient.Protobuf)
		if err != nil {
			panic(fmt.Sprintf("Error creating the schema %s", err))
		}
	}
	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))

	c.SubscribeTopics([]string{"dc1.identity.cdc.users"}, nil)
	go func() {
		for {
			msg, err := c.ReadMessage(-1)
			if err == nil {
				fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
				var event UserEvent
				err := json.Unmarshal(msg.Value, &event)
				if err != nil {
					fmt.Println("decode event error", err)
				}

				switch eventName := event.EventName; eventName {
				case "user.created":
					log.Println("create user")
					err := conn.QueryRow(context.Background(), "insert into tasks_users(public_id, username, role) values($1,$2,$3)", event.Data.PublicId, event.Data.Username, event.Data.Role)
					if err != nil {
						log.Println("sql err", err)
					}
				case "user.updated":
					log.Println("update user")
					_, err := conn.Exec(context.Background(), "update tasks_users set username=$1, role=$2 where public_id=$3", event.Data.Username, event.Data.Role, event.Data.PublicId)
					if err != nil {
						log.Println("sql err", err)
					}
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
			Tasks       []Task
			CurrentUser User
		}

		currentUser, err := getCurrentUser(w, r, conn)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var tasks []Task
		var rows pgx.Rows
		if currentUser.Role == "admin" || currentUser.Role == "manager" {
			rows, err = conn.Query(context.Background(), "select id, COALESCE(user_id, 0), description, created_at, status from tasks order by created_at desc")
			if err != nil {
				log.Println("sql err", err)
			}
		} else {
			rows, err = conn.Query(context.Background(), "select id, COALESCE(user_id, 0), description, created_at, status from tasks where user_id = $1 order by created_at desc", currentUser.ID)
			if err != nil {
				log.Println("sql err", err)
			}
		}

		for rows.Next() {
			var task Task
			err := rows.Scan(&task.ID, &task.UserID, &task.Description, &task.CreatedAt, &task.Status)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			tasks = append(tasks, task)
		}

		for i, task := range tasks {
			if task.UserID != 0 {
				var username string
				err = conn.QueryRow(context.Background(), "select username from tasks_users where id=$1", task.UserID).Scan(&username)
				if err != nil {
					log.Println("sql err", err)
				}
				tasks[i].AssignName = username
			}
		}
		rows.Close()
		var data Data
		data.Tasks = tasks
		data.CurrentUser = currentUser

		tmpl, err := template.ParseFiles("static/list.html")
		tmpl.Execute(w, data)
	})

	http.HandleFunc("/add", func(w http.ResponseWriter, r *http.Request) {
		type Data struct {
			CurrentUser User
		}

		_, err := getCurrentUser(w, r, conn)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if r.Method == "POST" {
			r.ParseForm()
			description := r.FormValue("description")
			title := r.FormValue("title")
			jiraID := r.FormValue("jira_id")

			createdAt := time.Now()
			var id int64
			err := conn.QueryRow(context.Background(), "insert into tasks(title, jira_id, description, status, created_at) values($1,$2,$3,$4,$5) RETURNING id;", title, jiraID, description, "new", createdAt).Scan(&id)
			if err != nil {
				log.Println("sql error", err)
			}

			topic := "dc1.task.cdc.tasks"

			serializer := NewProtobufSerializer(schemaRegistryClient, topic)
			eventUuid := uuid.NewV4().String()
			message := pc.Task{
				Id:          id,
				Status:      "new",
				Title:       title,
				Jiraid:      jiraID,
				Description: description,
				CreatedAt:   time.Now().Unix(),
				Header: &pc.Header{
					ApplicationId:   "task-tracker",
					Timestamp:       time.Now().Unix(),
					MessageId:       eventUuid,
					EventDescriptor: "task.created",
				},
			}
			value, err := serializer.Serialize(&message)
			//eventJson, _ := json.Marshal(event)
			p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          value,
			}, nil)

			w.Header().Set("Location", "/dashboard")
			w.WriteHeader(http.StatusFound)
			return
		}
		filename := "static/add.html"
		file, err := os.Open(filename)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		defer file.Close()
		fi, _ := file.Stat()
		http.ServeContent(w, r, file.Name(), fi.ModTime(), file)
	})

	http.HandleFunc("/assign", func(w http.ResponseWriter, r *http.Request) {
		type Data struct {
			CurrentUser User
		}

		currentUser, err := getCurrentUser(w, r, conn)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if currentUser.Role != "admin" && currentUser.Role != "manager" {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		rows, err := conn.Query(context.Background(), "select id from tasks where status = 'new'")
		if err != nil {
			log.Println("sql err", err)
		}

		var tasks []Task
		for rows.Next() {
			var task Task
			err := rows.Scan(&task.ID)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			tasks = append(tasks, task)
		}

		rows, err = conn.Query(context.Background(), "select public_id from tasks_users where role = 'engineer'")
		if err != nil {
			log.Println("sql err", err)
		}

		var users []User
		for rows.Next() {
			var user User
			err := rows.Scan(&user.ID)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			users = append(users, user)
		}

		for _, task := range tasks {
			randomIndex := rand.Intn(len(users))
			pick := users[randomIndex]
			_, err := conn.Exec(context.Background(), "update tasks set user_id=$1, status=$2 where id=$2", pick.ID, "птичка в клетке", task.ID)
			if err != nil {
				log.Println("sql err", err)
			}

			var publicId string
			err = conn.QueryRow(context.Background(), "select public_id from tasks where id=$1", task.ID).Scan(&publicId)

			topic := "dc1.task.cdc.tasks.assigned"

			serializer := NewProtobufSerializer(schemaRegistryClient, topic)
			eventUuid := uuid.NewV4().String()
			message := pc.Task{
				PublicId:  publicId,
				UserId:    pick.PublicID,
				CreatedAt: time.Now().Unix(),
				Header: &pc.Header{
					ApplicationId:   "task-tracker",
					Timestamp:       time.Now().Unix(),
					MessageId:       eventUuid,
					EventDescriptor: "task.assigned",
				},
			}
			value, err := serializer.Serialize(&message)
			p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          value,
			}, nil)

		}

		w.Header().Set("Location", "/dashboard")
		w.WriteHeader(http.StatusFound)
		return
	})

	http.HandleFunc("/edit", func(w http.ResponseWriter, r *http.Request) {
		type Data struct {
			CurrentUser User
			Task        Task
			ID          string
		}
		_, err := getCurrentUser(w, r, conn)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if r.Method == "POST" {
			r.ParseForm()
			id := r.FormValue("id")
			status := r.FormValue("status")

			idForm, _ := strconv.ParseInt(id, 10, 64)
			_, err := conn.Exec(context.Background(), "update tasks set status=$1 where id=$2", status, idForm)
			if err != nil {
				log.Println("sql error", err)
			}

			if status == "просо в миске" {
				topic := "dc1.task.cdc.tasks.finished"

				serializer := NewProtobufSerializer(schemaRegistryClient, topic)
				eventUuid := uuid.NewV4().String()
				message := pc.Task{
					PublicId:  publicId,
					CreatedAt: time.Now().Unix(),
					Header: &pc.Header{
						ApplicationId:   "task-tracker",
						Timestamp:       time.Now().Unix(),
						MessageId:       eventUuid,
						EventDescriptor: "task.finished",
					},
				}
				value, err := serializer.Serialize(&message)
				//eventJson, _ := json.Marshal(event)
				p.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
					Value:          value,
				}, nil)
			}
			w.Header().Set("Location", "/dashboard")
			w.WriteHeader(http.StatusFound)
			return
		}
		var data Data
		var task Task

		keys, _ := r.URL.Query()["id"]

		id := keys[0]
		err = conn.QueryRow(context.Background(), "select id,description,status from tasks where id=$1", id).Scan(&task.ID, &task.Description, &task.Status)
		data.Task = task
		tmpl, err := template.ParseFiles("static/edit.html")
		tmpl.Execute(w, data)
	})

	http.HandleFunc("/pwd", func(w http.ResponseWriter, r *http.Request) {
		token, err := config.PasswordCredentialsToken(context.Background(), "test", "test")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		globalToken = token
		e := json.NewEncoder(w)
		e.SetIndent("", "  ")
		e.Encode(token)
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

	log.Println("Client is running at 9094 port.Please open http://localhost:9094")
	log.Fatal(http.ListenAndServe(":9094", nil))
}

func genCodeChallengeS256(s string) string {
	s256 := sha256.Sum256([]byte(s))
	return base64.URLEncoding.EncodeToString(s256[:])
}
