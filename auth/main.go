package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"text/template"
	"time"

	"github.com/go-oauth2/oauth2/v4/generates"

	"github.com/davecgh/go-spew/spew"
	"github.com/go-oauth2/oauth2/v4/errors"
	"github.com/go-oauth2/oauth2/v4/manage"
	"github.com/go-oauth2/oauth2/v4/models"
	"github.com/go-oauth2/oauth2/v4/server"
	"github.com/go-oauth2/oauth2/v4/store"
	"github.com/go-session/session"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jackc/pgx/v4"
	"golang.org/x/crypto/bcrypt"
)

var (
	dumpvar   bool
	idvar     string
	secretvar string
	domainvar string
	portvar   int
)

type User struct {
	ID       int64
	Username string
	Password string
	Role     string
}

func init() {
	flag.BoolVar(&dumpvar, "d", true, "Dump requests and responses")
	flag.StringVar(&idvar, "i", "task-tracker", "The client id being passed in")
	flag.StringVar(&secretvar, "s", "22222222", "The client secret being passed in")
	flag.StringVar(&domainvar, "r", "http://localhost:9094", "The domain of the redirect url")
	flag.IntVar(&portvar, "p", 9096, "the base port for the server")
}

func main() {
	flag.Parse()
	if dumpvar {
		log.Println("Dumping requests")
	}
	manager := manage.NewDefaultManager()
	manager.SetAuthorizeCodeTokenCfg(manage.DefaultAuthorizeCodeTokenCfg)

	// token store
	manager.MustTokenStorage(store.NewMemoryTokenStore())

	// generate jwt access token
	// manager.MapAccessGenerate(generates.NewJWTAccessGenerate("", []byte("00000000"), jwt.SigningMethodHS512))
	manager.MapAccessGenerate(generates.NewAccessGenerate())

	clientStore := store.NewClientStore()
	clientStore.Set(idvar, &models.Client{
		ID:     idvar,
		Secret: secretvar,
		Domain: domainvar,
	})
	manager.MapClientStorage(clientStore)

	srv := server.NewServer(server.NewConfig(), manager)
	urlExample := "postgres://user:passwd@localhost:5432/user"
	conn, err := pgx.Connect(context.Background(), urlExample)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

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

	srv.SetPasswordAuthorizationHandler(func(username, password string) (userID string, err error) {
		if username == "test" && password == "test" {
			userID = "test"
		}
		return
	})

	srv.SetUserAuthorizationHandler(userAuthorizeHandler)

	srv.SetInternalErrorHandler(func(err error) (re *errors.Response) {
		log.Println("Internal Error:", err.Error())
		return
	})

	srv.SetResponseErrorHandler(func(re *errors.Response) {
		log.Println("Response Error:", re.Error.Error())
	})

	http.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {

		if dumpvar {
			_ = dumpRequest(os.Stdout, "login", r) // Ignore the error
		}
		store, err := session.Start(r.Context(), w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if r.Method == "POST" {
			r.ParseForm()
			username := r.FormValue("username")
			var user User
			err = conn.QueryRow(context.Background(), "select id,username,password,role from users where username=$1", username).Scan(&user.ID, &user.Username, &user.Password, &user.Role)
			password := r.FormValue("password")
			if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(password)); err != nil {
				http.Error(w, "Invalid auth", http.StatusInternalServerError)
				return
			}
			store.Set("user_id", user.ID)
			store.Set("username", user.Username)
			store.Set("role", user.Role)
			store.Save()

			w.Header().Set("Location", "/auth")
			w.WriteHeader(http.StatusFound)
			return
		}
		outputHTML(w, r, "static/login.html")
	})

	http.HandleFunc("/list", func(w http.ResponseWriter, r *http.Request) {
		type Data struct {
			Users       []User
			CurrentUser User
		}

		var currentUser User
		if dumpvar {
			_ = dumpRequest(os.Stdout, "list", r) // Ignore the error
		}
		store, err := session.Start(r.Context(), w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if v, ok := store.Get("user_id"); ok {
			currentUser.ID = v.(int64)
		} else {
			log.Println("GO TO LOGIN")
			http.Redirect(w, r, "/login", http.StatusFound)
		}

		if v, ok := store.Get("role"); ok {
			currentUser.Role = v.(string)
		}
		if v, ok := store.Get("username"); ok {
			currentUser.Username = v.(string)
		}

		users := []User{}
		rows, err := conn.Query(context.Background(), "select id, username, role from users")
		if err != nil {
			log.Println("sql err", err)
		}

		for rows.Next() {
			var user User
			err := rows.Scan(&user.ID, &user.Username, &user.Role)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			users = append(users, user)
		}
		var data Data
		data.Users = users
		data.CurrentUser = currentUser

		tmpl, err := template.ParseFiles("static/list.html")
		tmpl.Execute(w, data)

	})
	http.HandleFunc("/add", func(w http.ResponseWriter, r *http.Request) {
		type Data struct {
			CurrentUser User
		}

		var currentUser User
		if dumpvar {
			_ = dumpRequest(os.Stdout, "list", r) // Ignore the error
		}
		store, err := session.Start(r.Context(), w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if v, ok := store.Get("user_id"); ok {
			currentUser.ID = v.(int64)
		} else {
			http.Redirect(w, r, "/login", http.StatusFound)
		}

		if v, ok := store.Get("role"); ok {
			currentUser.Role = v.(string)
		}
		if v, ok := store.Get("username"); ok {
			currentUser.Username = v.(string)
		}

		if currentUser.Role != "admin" {
			http.Redirect(w, r, "/list", http.StatusFound)
		}

		if r.Method == "POST" {
			r.ParseForm()
			username := r.FormValue("username")
			password := r.FormValue("password")
			role := r.FormValue("role")

			hashedPassword, _ := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)

			var id int64
			err := conn.QueryRow(context.Background(), "insert into users(username, password, role) values($1,$2,$3) RETURNING id;", username, hashedPassword, role).Scan(&id)
			if err != nil {
				log.Println("sql error", err)
			}

			topic := "users-stream"

			eventData := make(map[string]interface{})
			eventData["id"] = id
			eventData["username"] = username
			eventData["role"] = role

			event := make(map[string]interface{})
			event["event_name"] = "UserCreated"
			event["data"] = eventData

			eventJson, _ := json.Marshal(event)
			p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          []byte(eventJson),
			}, nil)

			w.Header().Set("Location", "/list")
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

	http.HandleFunc("/edit", func(w http.ResponseWriter, r *http.Request) {
		type Data struct {
			CurrentUser User
			User        User
			ID          string
		}

		var currentUser User
		if dumpvar {
			_ = dumpRequest(os.Stdout, "list", r) // Ignore the error
		}
		store, err := session.Start(r.Context(), w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if v, ok := store.Get("user_id"); ok {
			currentUser.ID = v.(int64)
		} else {
			http.Redirect(w, r, "/login", http.StatusFound)
		}

		if v, ok := store.Get("role"); ok {
			currentUser.Role = v.(string)
		}
		if v, ok := store.Get("username"); ok {
			currentUser.Username = v.(string)
		}

		if currentUser.Role != "admin" {
			http.Redirect(w, r, "/list", http.StatusFound)
		}

		if r.Method == "POST" {
			r.ParseForm()
			id := r.FormValue("id")
			username := r.FormValue("username")
			role := r.FormValue("role")

			idForm, _ := strconv.ParseInt(id, 10, 64)
			fmt.Println("update", username, role, idForm)
			_, err := conn.Exec(context.Background(), "update users set username=$1, role=$2 where id=$3", username, role, idForm)
			if err != nil {
				log.Println("sql error", err)
			}
			topic := "users-stream"

			eventData := make(map[string]interface{})
			eventData["id"] = idForm
			eventData["username"] = username
			eventData["role"] = role

			event := make(map[string]interface{})
			event["event_name"] = "UserUpdated"
			event["data"] = eventData

			eventJson, _ := json.Marshal(event)
			p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          []byte(eventJson),
			}, nil)
			w.Header().Set("Location", "/list")
			w.WriteHeader(http.StatusFound)
			return
		}
		var data Data
		var user User

		keys, _ := r.URL.Query()["id"]

		id := keys[0]
		fmt.Println("idd", id)
		err = conn.QueryRow(context.Background(), "select id,username,role from users where id=$1", id).Scan(&user.ID, &user.Username, &user.Role)
		data.User = user
		tmpl, err := template.ParseFiles("static/edit.html")
		tmpl.Execute(w, data)
	})

	http.HandleFunc("/auth", authHandler)

	http.HandleFunc("/oauth/authorize", func(w http.ResponseWriter, r *http.Request) {
		if dumpvar {
			dumpRequest(os.Stdout, "authorize", r)
		}

		store, err := session.Start(r.Context(), w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var form url.Values
		if v, ok := store.Get("ReturnUri"); ok {
			form = v.(url.Values)
			spew.Dump(form)
		}
		r.Form = form

		store.Delete("ReturnUri")
		store.Save()

		err = srv.HandleAuthorizeRequest(w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
	})

	http.HandleFunc("/oauth/token", func(w http.ResponseWriter, r *http.Request) {
		if dumpvar {
			_ = dumpRequest(os.Stdout, "token", r) // Ignore the error
		}

		err := srv.HandleTokenRequest(w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	http.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
		if dumpvar {
			_ = dumpRequest(os.Stdout, "test", r) // Ignore the error
		}
		token, err := srv.ValidationBearerToken(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		data := map[string]interface{}{
			"expires_in": int64(token.GetAccessCreateAt().Add(token.GetAccessExpiresIn()).Sub(time.Now()).Seconds()),
			"client_id":  token.GetClientID(),
			"user_id":    token.GetUserID(),
		}
		e := json.NewEncoder(w)
		e.SetIndent("", "  ")
		e.Encode(data)
	})

	log.Printf("Server is running at %d port.\n", portvar)
	log.Printf("Point your OAuth client Auth endpoint to %s:%d%s", "http://localhost", portvar, "/oauth/authorize")
	log.Printf("Point your OAuth client Token endpoint to %s:%d%s", "http://localhost", portvar, "/oauth/token")
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", portvar), nil))
}

func dumpRequest(writer io.Writer, header string, r *http.Request) error {
	data, err := httputil.DumpRequest(r, true)
	if err != nil {
		return err
	}
	writer.Write([]byte("\n" + header + ": \n"))
	writer.Write(data)
	return nil
}

func userAuthorizeHandler(w http.ResponseWriter, r *http.Request) (userID string, err error) {
	if dumpvar {
		_ = dumpRequest(os.Stdout, "userAuthorizeHandler", r) // Ignore the error
	}
	store, err := session.Start(r.Context(), w, r)
	if err != nil {
		return
	}

	uid, ok := store.Get("user_id")
	if !ok {
		if r.Form == nil {
			r.ParseForm()
		}

		store.Set("ReturnUri", r.Form)
		store.Save()

		w.Header().Set("Location", "/login")
		w.WriteHeader(http.StatusFound)
		return
	}

	userID = strconv.FormatInt(uid.(int64), 10)
	store.Delete("user_id")
	store.Save()
	return
}

func authHandler(w http.ResponseWriter, r *http.Request) {
	if dumpvar {
		_ = dumpRequest(os.Stdout, "auth", r) // Ignore the error
	}
	store, err := session.Start(nil, w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if _, ok := store.Get("user_id"); !ok {
		w.Header().Set("Location", "/login")
		w.WriteHeader(http.StatusFound)
		return
	}

	outputHTML(w, r, "static/auth.html")
}

func outputHTML(w http.ResponseWriter, req *http.Request, filename string) {
	file, err := os.Open(filename)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer file.Close()
	fi, _ := file.Stat()
	http.ServeContent(w, req, file.Name(), fi.ModTime(), file)
}
