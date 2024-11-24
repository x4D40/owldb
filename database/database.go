package database

import (
	"fmt"
	"log"
	"net/http"
	"owldb/database/partition"

	"github.com/hashicorp/serf/serf"
)

type Database struct {
	config  Config
	node    *serf.Serf
	eventCh chan serf.Event
	ring    *partition.HashRing
	server  *http.Server
	storage map[string]string
}

func NewDatabase(config Config) *Database {

	db := &Database{
		config:  config,
		storage: make(map[string]string),
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		value := r.URL.Query().Get("value")
		db.storage[key] = value

		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		value, exists := db.storage[key]

		if exists {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, value)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	})

	server := &http.Server{
		Addr: fmt.Sprintf(":%d", config.Port),
	}

	db.server = server

	return db
}

/*
Joins the database to a cluster
*/
func (db *Database) Join() error {
	config := serf.DefaultConfig()

	eventCh := make(chan serf.Event)

	config.NodeName = db.config.Id
	config.MemberlistConfig.BindAddr = db.config.Host
	config.MemberlistConfig.BindPort = db.config.GossipPort
	config.EventCh = eventCh

	node, err := serf.Create(config)

	if err != nil {
		return err
	}

	// for now just join, need to replicate data later
	db.node = node
	db.eventCh = eventCh
	db.ring = partition.NewHashRing(29)

	go db.handleGossipEvents()

	if _, err = node.Join(db.config.Join, false); err != nil {
		return err
	}

	return nil
}

func (db *Database) Leave() error {
	// todo: error if no node
	return db.node.Leave()
}

func (db *Database) handleGossipEvents() {
	for e := range db.eventCh {

		switch e.EventType() {
		case serf.EventMemberJoin:
			evt := e.(serf.MemberEvent)
			log.Println(evt.String())

			for _, member := range evt.Members {
				db.ring.AddNode(&partition.Node{
					Id:   member.Name,
					Host: member.Addr.String(),
					Port: member.Port,
				})
			}

		case serf.EventMemberLeave:
			evt := e.(serf.MemberEvent)
			log.Println(evt.String())
		case serf.EventMemberFailed:
			evt := e.(serf.MemberEvent)
			log.Println(evt.String())
		case serf.EventMemberUpdate:
			evt := e.(serf.MemberEvent)
			log.Println(evt.String())
		case serf.EventMemberReap:
			evt := e.(serf.MemberEvent)
			log.Println(evt.String())
		case serf.EventUser:
			evt := e.(serf.UserEvent)
			log.Println(evt.String())
		case serf.EventQuery:
			evt := e.(*serf.Query)
			log.Println(evt.String())
		default:
			log.Panicf("Unknown serf event type %s : %s", e.EventType(), e.String())
		}
	}
}
