package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
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

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-session/session"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

const (
	authServerURL = "http://localhost:9096"
)

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

	c.SubscribeTopics([]string{"users-stream"}, nil)
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
				case "UserCreated":
					log.Println("create user")
					err := conn.QueryRow(context.Background(), "insert into tasks_users(id, username, role) values($1,$2,$3)", event.Data.ID, event.Data.Username, event.Data.Role)
					if err != nil {
						log.Println("sql err", err)
					}
				case "UserUpdated":
					log.Println("update user")
					_, err := conn.Exec(context.Background(), "update tasks_users set username=$1, role=$2 where id=$3", event.Data.Username, event.Data.Role, event.Data.ID)
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

			createdAt := time.Now()
			var id int64
			err := conn.QueryRow(context.Background(), "insert into tasks(description, status, created_at) values($1,$2,$3) RETURNING id;", description, "new", createdAt).Scan(&id)
			if err != nil {
				log.Println("sql error", err)
			}

			topic := "tasks-stream"

			eventData := make(map[string]interface{})
			eventData["id"] = id
			eventData["status"] = "new"
			eventData["description"] = description
			eventData["created_at"] = createdAt

			event := make(map[string]interface{})
			event["event_name"] = "TaskCreated"
			event["data"] = eventData

			eventJson, _ := json.Marshal(event)
			p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          []byte(eventJson),
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

		rows, err = conn.Query(context.Background(), "select id from tasks_users where role = 'engineer'")
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
			_, err := conn.Exec(context.Background(), "update tasks set user_id=$1 where id=$2", pick.ID, task.ID)
			if err != nil {
				log.Println("sql err", err)
			}

			topic := "tasks-stream"

			eventData := make(map[string]interface{})
			eventData["id"] = task.ID
			eventData["user_id"] = pick.ID
			eventData["created_at"] = time.Now()

			event := make(map[string]interface{})
			event["event_name"] = "TaskAssigned"
			event["data"] = eventData

			eventJson, _ := json.Marshal(event)
			p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          []byte(eventJson),
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
			topic := "tasks-stream"

			eventData := make(map[string]interface{})
			eventData["id"] = idForm
			eventData["status"] = status
			eventData["created_at"] = time.Now()

			event := make(map[string]interface{})
			event["event_name"] = "TaskUpdated"
			event["data"] = eventData

			eventJson, _ := json.Marshal(event)
			p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          []byte(eventJson),
			}, nil)
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
