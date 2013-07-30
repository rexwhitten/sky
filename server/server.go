package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/benbjohnson/go-raft"
	"github.com/gorilla/mux"
	"github.com/skydb/sky"
	"github.com/skydb/sky/core"
	"github.com/skydb/sky/factors"
	"github.com/skydb/sky/query"
	"github.com/skydb/sky/query/engine"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"sync"
	"time"
)

//------------------------------------------------------------------------------
//
// Globals
//
//------------------------------------------------------------------------------

const (
	DefaultElectionTimeout  = 2 * time.Second
	DefaultHeartbeatTimeout = 1 * time.Second
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A Server is the front end that controls access to tables.
type Server struct {
	httpServer        *http.Server
	clusterRaftServer *raft.Server
	groupRaftServer   *raft.Server
	cluster           *Cluster
	router            *mux.Router
	logger            *log.Logger
	nodeId            string
	nodeGroupId       string
	path              string
	host              string
	port              uint
	listener          net.Listener
	servlets          []*Servlet
	tables            map[string]*core.Table
	fdb               *factors.DB
	shutdownChannel   chan bool
	shutdownFinished  chan bool
	mutex             sync.Mutex
	ElectionTimeout   time.Duration
	HeartbeatTimeout  time.Duration
}

//------------------------------------------------------------------------------
//
// Errors
//
//------------------------------------------------------------------------------

//--------------------------------------
// Alternate Content Type
//--------------------------------------

// Hackish way to return plain text for query codegen. Might fix later but
// it's rarely used.
type TextPlainContentTypeError struct {
}

func (e *TextPlainContentTypeError) Error() string {
	return ""
}

//------------------------------------------------------------------------------
//
// Constructors
//
//------------------------------------------------------------------------------

// NewServer returns a new Server.
func NewServer(port uint, path string) (*Server, error) {
	// Retrieve the host information.
	host, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("skyd.Server: Unable to determine hostname: %s", err)
	}

	r := mux.NewRouter()
	s := &Server{
		httpServer:       &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: r},
		router:           r,
		logger:           log.New(os.Stdout, "", log.LstdFlags),
		path:             path,
		host:             host,
		port:             port,
		tables:           make(map[string]*core.Table),
		ElectionTimeout:  DefaultElectionTimeout,
		HeartbeatTimeout: DefaultHeartbeatTimeout,
	}

	s.addHandlers()
	s.addTableHandlers()
	s.addPropertyHandlers()
	s.addEventHandlers()
	s.addObjectHandlers()
	s.addQueryHandlers()
	s.addClusterHandlers()
	s.addDebugHandlers()

	return s, nil
}

//------------------------------------------------------------------------------
//
// Properties
//
//------------------------------------------------------------------------------

// The root server path.
func (s *Server) Path() string {
	return s.path
}

// The path to the file containing the server's node id.
func (s *Server) NodeIdPath() string {
	return fmt.Sprintf("%v/node", s.path)
}

// The path to the file containing the server's node group id.
func (s *Server) NodeGroupIdPath() string {
	return fmt.Sprintf("%v/group", s.path)
}

// The path to the data directory.
func (s *Server) DataPath() string {
	return fmt.Sprintf("%v/data", s.path)
}

// The path to the table metadata directory.
func (s *Server) TablesPath() string {
	return fmt.Sprintf("%v/tables", s.path)
}

// Generates the path for a table attached to the server.
func (s *Server) TablePath(name string) string {
	return fmt.Sprintf("%v/%v", s.TablesPath(), name)
}

// The path to the factors database.
func (s *Server) FactorsPath() string {
	return fmt.Sprintf("%v/factors", s.path)
}

// The path to the binlog directory. This is used to log updates to the server
// for the purpose of replication.
func (s *Server) BinlogPath() string {
	return fmt.Sprintf("%v/binlog", s.path)
}

// The path to the raft directory for the cluster.
func (s *Server) ClusterRaftPath() string {
	return fmt.Sprintf("%v/raft/cluster", s.path)
}

// The path to the raft directory for the group.
func (s *Server) GroupRaftPath() string {
	return fmt.Sprintf("%v/raft/group", s.path)
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// Lifecycle
//--------------------------------------

// Runs the server.
func (s *Server) ListenAndServe(shutdownChannel chan bool) error {
	s.shutdownChannel = shutdownChannel

	err := s.open()
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		s.close()
		return err
	}
	s.listener = listener

	s.shutdownFinished = make(chan bool)
	go func() {
		s.httpServer.Serve(s.listener)
		s.shutdownFinished <- true
		s.shutdownFinished = nil
	}()

	s.logger.Printf("Sky v%s is now listening on http://localhost%s\n", sky.Version, s.httpServer.Addr)

	return nil
}

// Stops the server.
func (s *Server) Shutdown() error {
	if !s.Running() {
		return nil
	}

	// Close servlets.
	s.close()

	// Close socket.
	if s.listener != nil {
		// Then stop the server.
		err := s.listener.Close()
		s.listener = nil
		if err != nil {
			return err
		}
	}

	// wait for server goroutine to finish
	if s.shutdownFinished != nil {
		<-s.shutdownFinished
	}

	// Notify that the server is shutdown.
	if s.shutdownChannel != nil {
		s.shutdownChannel <- true
	}

	return nil
}

// Checks if the server is listening for new connections.
func (s *Server) Running() bool {
	return (s.listener != nil)
}

// Opens the data directory and servlets.
func (s *Server) open() error {
	if s.Running() {
		s.close()
	}

	// Setup the file system if it doesn't exist.
	err := s.createIfNotExists()
	if err != nil {
		return fmt.Errorf("skyd.Server: Unable to create server folders: %v", err)
	}

	// Read in the server's node id or generate one.
	if nodeId, err := ioutil.ReadFile(s.NodeIdPath()); err == nil {
		s.nodeId = string(nodeId)
	} else {
		s.nodeId = NewNodeId()
		if err = ioutil.WriteFile(s.NodeIdPath(), []byte(s.nodeId), 0644); err != nil {
			panic("skyd: Unable to write node file")
		}
	}

	// Read in the server's node group id or generate one.
	if nodeGroupId, err := ioutil.ReadFile(s.NodeGroupIdPath()); err == nil {
		s.nodeGroupId = string(nodeGroupId)
	} else {
		s.nodeGroupId = NewNodeGroupId()
		if err = ioutil.WriteFile(s.NodeGroupIdPath(), []byte(s.nodeGroupId), 0644); err != nil {
			panic("skyd: Unable to write group file")
		}
	}

	// Open factors database.
	s.fdb = factors.NewDB(s.FactorsPath())
	err = s.fdb.Open()
	if err != nil {
		s.close()
		return err
	}

	// Create servlets from child directories with numeric names.
	infos, err := ioutil.ReadDir(s.DataPath())
	if err != nil {
		return err
	}
	for _, info := range infos {
		match, _ := regexp.MatchString("^\\d$", info.Name())
		if info.IsDir() && match {
			s.servlets = append(s.servlets, NewServlet(fmt.Sprintf("%s/%s", s.DataPath(), info.Name()), s.fdb))
		}
	}

	// If none exist then build them based on the number of logical CPUs available.
	if len(s.servlets) == 0 {
		cpuCount := runtime.NumCPU()
		for i := 0; i < cpuCount; i++ {
			s.servlets = append(s.servlets, NewServlet(fmt.Sprintf("%s/%v", s.DataPath(), i), s.fdb))
		}
	}

	// Initialize consensus server.
	if err = s.initRaftServers(); err != nil {
		s.close()
		return err
	}

	// Open servlets.
	for _, servlet := range s.servlets {
		err = servlet.Open()
		if err != nil {
			s.close()
			return err
		}
	}

	return nil
}

// Sets up and starts the cluster and group consensus servers.
func (s *Server) initRaftServers() error {
	if err := s.initClusterRaftServer(true); err != nil {
		return err
	}
	if err := s.initGroupRaftServer(true); err != nil {
		return err
	}
	return nil
}

// Sets up and starts the cluster and group consensus servers.
func (s *Server) initClusterRaftServer(initSingleNode bool) error {
	// Create the log directory.
	if err := os.MkdirAll(s.ClusterRaftPath(), 0700); err != nil {
		return err
	}

	// Initialize empty cluster.
	s.cluster = NewCluster()

	// Setup the consensus server.
	var err error
	if s.clusterRaftServer, err = raft.NewServer(s.nodeId, s.ClusterRaftPath(), s, s); err != nil {
		return err
	}
	s.clusterRaftServer.SetElectionTimeout(s.ElectionTimeout)
	s.clusterRaftServer.SetHeartbeatTimeout(s.HeartbeatTimeout)

	// Start the consensus server.
	if err = s.clusterRaftServer.Start(); err != nil {
		return err
	}

	// If this is a new server then add a group and a single node.
	if initSingleNode && s.clusterRaftServer.IsLogEmpty() {
		s.clusterRaftServer.Initialize()
		if err = s.clusterRaftServer.Do(NewCreateNodeGroupCommand(s.nodeGroupId)); err != nil {
			return err
		}
		if err = s.clusterRaftServer.Do(NewCreateNodeCommand(s.nodeId, s.nodeGroupId, s.host, s.port)); err != nil {
			return err
		}
		if err = s.clusterRaftServer.Do(NewInitCommand()); err != nil {
			return err
		}
	}

	return nil
}

// Initializes the consensus server for the node's group.
func (s *Server) initGroupRaftServer(initSingleNode bool) error {
	// Create the log directory.
	if err := os.MkdirAll(s.GroupRaftPath(), 0700); err != nil {
		return err
	}

	// Setup the consensus server.
	var err error
	if s.groupRaftServer, err = raft.NewServer(s.nodeId, s.GroupRaftPath(), s, s); err != nil {
		return err
	}
	s.groupRaftServer.SetElectionTimeout(s.ElectionTimeout)
	s.groupRaftServer.SetHeartbeatTimeout(s.HeartbeatTimeout)

	// Start the consensus server.
	if err = s.groupRaftServer.Start(); err != nil {
		return err
	}

	if initSingleNode && s.groupRaftServer.IsLogEmpty() {
		s.groupRaftServer.Initialize()
	}

	// TODO: Initialize peers from cluster raft server.

	return nil
}

// Closes the data directory and servlets.
func (s *Server) close() {
	// Close servlets.
	if s.servlets != nil {
		for _, servlet := range s.servlets {
			servlet.Close()
		}
		s.servlets = nil
	}

	// Close factors database.
	if s.fdb != nil {
		s.fdb.Close()
		s.fdb = nil
	}

	// Close consensus servers.
	s.closeClusterRaftServer()
	s.closeGroupRaftServer()
	s.nodeId = ""
	s.nodeGroupId = ""
}

func (s *Server) closeClusterRaftServer() {
	if s.clusterRaftServer != nil {
		s.clusterRaftServer.Stop()
		s.clusterRaftServer = nil
	}
	s.cluster = nil
}

func (s *Server) closeGroupRaftServer() {
	if s.groupRaftServer != nil {
		s.groupRaftServer.Stop()
		s.groupRaftServer = nil
	}
}

// Creates the appropriate directory structure if one does not exist.
func (s *Server) createIfNotExists() error {
	if err := os.MkdirAll(s.path, 0700); err != nil {
		return err
	}
	if err := os.MkdirAll(s.DataPath(), 0700); err != nil {
		return err
	}
	if err := os.MkdirAll(s.TablesPath(), 0700); err != nil {
		return err
	}
	return nil
}

//--------------------------------------
// Logging
//--------------------------------------

// Silences the log.
func (s *Server) Silence() {
	s.logger = log.New(ioutil.Discard, "", log.LstdFlags)
}

//--------------------------------------
// Routing
//--------------------------------------

// Parses incoming JSON objects and converts outgoing responses to JSON.
func (s *Server) ApiHandleFunc(route string, obj interface{}, handlerFunction func(http.ResponseWriter, *http.Request, interface{}) (interface{}, error)) *mux.Route {
	wrappedFunction := func(w http.ResponseWriter, req *http.Request) {
		// warn("%s \"%s %s %s\"", req.RemoteAddr, req.Method, req.RequestURI, req.Proto)
		t0 := time.Now()

		// Make sure there's not a lingering connection open after shutdown.
		if !s.Running() {
			warn("[%p] not running", s)
			w.WriteHeader(http.StatusGone)
			return
		}

		// Copy the serialization object.
		var err error
		var params interface{}
		if obj == nil {
			params = make(map[string]interface{})
			err = s.decodeParams(w, req, &params)
		} else {
			params = reflect.New(reflect.Indirect(reflect.ValueOf(obj)).Type()).Interface()
			err = s.decodeParams(w, req, params)
		}

		// Execute the handler.
		var ret interface{}
		if err == nil {
			ret, err = handlerFunction(w, req, params)
		}

		// If we're returning plain text then just dump out what's returned.
		if _, ok := err.(*TextPlainContentTypeError); ok {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			if str, ok := ret.(string); ok {
				w.Write([]byte(str))
			}
			return
		}

		// If there is an error then replace the return value.
		if err != nil {
			w.Header().Set("Sky-Error-Message", err.Error())
			ret = map[string]interface{}{"message": err.Error()}
		}

		// Write header status.
		w.Header().Set("Content-Type", "application/json")
		var status int
		if err == nil {
			status = http.StatusOK
		} else {
			status = http.StatusInternalServerError
		}
		w.WriteHeader(status)

		// Write to access log.
		s.logger.Printf("%s \"%s %s %s\" %d %0.3f", req.RemoteAddr, req.Method, req.RequestURI, req.Proto, status, time.Since(t0).Seconds())
		if status != http.StatusOK {
			s.logger.Printf("ERROR %v", err)
		}

		// Encode the return value appropriately.
		if ret != nil {
			encoder := json.NewEncoder(w)
			err := encoder.Encode(ConvertToStringKeys(ret))
			if err != nil {
				fmt.Printf("skyd.Server: Encoding error: %v\n", err.Error())
				return
			}
		}
	}

	return s.router.HandleFunc(route, wrappedFunction)
}

// Decodes the body of the message into parameters.
func (s *Server) decodeParams(w http.ResponseWriter, req *http.Request, params interface{}) error {
	decoder := json.NewDecoder(req.Body)
	err := decoder.Decode(&params)
	if err != nil && err != io.EOF {
		return fmt.Errorf("skyd: Malformed json request: %v", err)
	}
	return nil
}

//--------------------------------------
// Servlet Management
//--------------------------------------

// Retrieves the table and servlet reference for a single object.
func (s *Server) GetObjectContext(tableName string, objectId string) (*core.Table, *Servlet, error) {
	// Return an error if the table already exists.
	table, err := s.OpenTable(tableName)
	if err != nil {
		return nil, nil, err
	}

	// Determine servlet index.
	index := s.GetObjectServletIndex(table, objectId)
	servlet := s.servlets[index]

	return table, servlet, nil
}

// Calculates a tablet index based on the object identifier even hash.
func (s *Server) GetObjectServletIndex(t *Table, objectId string) uint32 {
	return ObjectLocalHash(objectId) % uint32(len(s.servlets))
}

//--------------------------------------
// Table Management
//--------------------------------------

// Retrieves a table that has already been opened.
func (s *Server) GetTable(name string) *core.Table {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.tables[name]
}

// Retrieves a list of all tables in the database but does not open them.
// Do not use these table references for anything but informational purposes!
func (s *Server) GetAllTables() ([]*core.Table, error) {
	// Create a table object for each directory in the tables path.
	infos, err := ioutil.ReadDir(s.TablesPath())
	if err != nil {
		return nil, err
	}

	tables := []*core.Table{}
	for _, info := range infos {
		if info.IsDir() {
			tables = append(tables, core.NewTable(info.Name(), s.TablePath(info.Name())))
		}
	}

	return tables, nil
}

// Opens a table and returns a reference to it.
func (s *Server) OpenTable(name string) (*core.Table, error) {
	// If table already exists then use it.
	table := s.GetTable(name)
	if table != nil {
		return table, nil
	}

	// Otherwise open it and save the reference.
	table = core.NewTable(name, s.TablePath(name))
	err := table.Open()
	if err != nil {
		table.Close()
		return nil, err
	}

	s.mutex.Lock()
	s.tables[name] = table
	s.mutex.Unlock()

	return table, nil
}

// Deletes a table.
func (s *Server) DeleteTable(name string) error {
	// Return an error if the table doesn't exist.
	table := s.GetTable(name)
	if table == nil {
		table = core.NewTable(name, s.TablePath(name))
	}
	if !table.Exists() {
		return fmt.Errorf("Table does not exist: %s", name)
	}

	// Delete data from each servlet.
	for _, servlet := range s.servlets {
		if err := servlet.DeleteTable(name); err != nil {
			return err
		}
	}

	// Remove the table from the lookup and remove it's core.
	s.mutex.Lock()
	delete(s.tables, name)
	defer s.mutex.Unlock()

	return table.Delete()
}

//--------------------------------------
// Query
//--------------------------------------

// Runs a query against a table.
func (s *Server) RunQuery(table *core.Table, q *query.Query) (interface{}, error) {
	var rootEngine *engine.ExecutionEngine
	engines := make([]*engine.ExecutionEngine, 0)

	// Create a channel to receive aggregate responses.
	rchannel := make(chan interface{}, len(s.servlets))

	// Generate the query source code.
	source, err := q.Codegen()
	if err != nil {
		return nil, err
	}

	// Create an engine for merging results.
	rootEngine, err = engine.NewExecutionEngine(table, q.Prefix, source)
	if err != nil {
		return nil, err
	}
	defer rootEngine.Destroy()
	//fmt.Println(engine.FullAnnotatedSource())

	// Initialize one execution engine for each servlet.
	var data interface{}
	for index, servlet := range s.servlets {
		// Create an engine for each servlet. The execution engine is
		// protected by a mutex so it's safe to destroy it at any time.
		subengine, err := servlet.CreateExecutionEngine(table, q.Prefix, source)
		if err != nil {
			return nil, err
		}
		defer subengine.Destroy()
		engines = append(engines, subengine)

		// Run initialization once if required.
		if index == 0 && q.RequiresInitialization() {
			if data, err = subengine.Initialize(); err != nil {
				return nil, err
			}
			// Reset the iterator.
			if err = subengine.ResetLmdbCursor(); err != nil {
				return nil, err
			}
		}
	}

	// Execute servlets asynchronously and retrieve responses outside
	// of the server context.
	for index, _ := range s.servlets {
		e := engines[index]
		go func() {
			if result, err := e.Aggregate(data); err != nil {
				rchannel <- err
			} else {
				rchannel <- result
			}
		}()
	}

	// Wait for each servlet to complete and then merge the results.
	var servletError error
	var result interface{}
	result = make(map[interface{}]interface{})
	for i := 0; i < len(s.servlets); i++ {
		ret := <-rchannel
		if err, ok := ret.(error); ok {
			fmt.Printf("skyd.Server: Aggregate error: %v", err)
			servletError = err
		} else {
			// Defactorize aggregate results.
			if err = q.Defactorize(ret); err != nil {
				return nil, err
			}

			// Merge results.
			if ret != nil {
				if result, err = rootEngine.Merge(result, ret); err != nil {
					fmt.Printf("skyd.Server: Merge error: %v", err)
					servletError = err
				}
			}
		}
	}
	err = servletError

	return result, err
}

//--------------------------------------
// RPC
//--------------------------------------

//--------------------------------------
// Membership
//--------------------------------------

// Attempts to join a cluster that a given host is a part of.
func (s *Server) Join(host string, port uint) error {
	// TODO: Make sure server has no data or log before attempting to join.
	return rpc(host, port, "POST", "/cluster/nodes", map[string]interface{}{"nodeId": s.nodeId, "host": s.host, "port": s.port}, nil)
}

// Attempts to leave cluster.
func (s *Server) Leave() error {
	// Determine the cluster leader.
	leaderNodeId := s.clusterRaftServer.Leader()
	host, port, err := s.cluster.GetNodeHostname(s.clusterRaftServer.Leader())
	if err != nil {
		return fmt.Errorf("skyd: Unable to find leader: %v (%v)", leaderNodeId, err)
	}

	return rpc(host, port, "DELETE", fmt.Sprintf("/cluster/nodes/%s", s.nodeId), nil, nil)
}

//--------------------------------------
// Cluster-level Raft Execution
//--------------------------------------

// Retrieves the number of members in the cluster-level Raft server.
func (s *Server) ClusterRaftMemberCount() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.clusterRaftServer != nil {
		return s.clusterRaftServer.MemberCount()
	}
	return 0
}

// Retrieves the leader node id and host/port.
func (s *Server) GetClusterLeaderHostname() (string, string, uint, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.clusterRaftServer == nil || s.clusterRaftServer.State() == raft.Stopped {
		return "", "", 0, fmt.Errorf("Cluster raft server unavailable")
	}
	if s.cluster == nil {
		return "", "", 0, fmt.Errorf("Cluster data unavailable")
	}

	leaderNodeId := s.clusterRaftServer.Leader()
	host, port, err := s.cluster.GetNodeHostname(leaderNodeId)
	if err != nil {
		return "", "", 0, fmt.Errorf("Unable to find leader: %v (%v)", leaderNodeId, err)
	}
	return leaderNodeId, host, port, nil
}

// Executes a command on the cluster-level state machine.
func (s *Server) ExecuteClusterCommand(command raft.Command) error {
	raftServer := s.clusterRaftServer
	leaderNodeId, host, port, err := s.GetClusterLeaderHostname()
	if err != nil {
		return err
	}

	// Forward to leader if we're not the leader.
	if leaderNodeId != s.nodeId {
		return rpc(host, port, "POST", "/cluster/commands", command, nil)
	}

	// Apply to this node if we're leader.
	err = raftServer.Do(command)
	return err
}

//--------------------------------------
// Cluster-level Raft Execution
//--------------------------------------

// Retrieves the number of members in the group-level Raft server.
func (s *Server) GroupRaftMemberCount() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.groupRaftServer != nil {
		return s.groupRaftServer.MemberCount()
	}
	return 0
}

// Retrieves the leader node id and host/port.
func (s *Server) GetGroupLeaderHostname() (string, string, uint, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.groupRaftServer == nil || s.groupRaftServer.State() == raft.Stopped {
		return "", "", 0, fmt.Errorf("Group raft server unavailable")
	}
	if s.cluster == nil {
		return "", "", 0, fmt.Errorf("Cluster data unavailable")
	}

	leaderNodeId := s.groupRaftServer.Leader()
	host, port, err := s.cluster.GetNodeHostname(leaderNodeId)
	if err != nil {
		return "", "", 0, fmt.Errorf("Unable to find leader: %v (%v)", leaderNodeId, err)
	}
	return leaderNodeId, host, port, nil
}

// Executes a command on the group-level state machine.
func (s *Server) ExecuteGroupCommand(command raft.Command) error {
	raftServer := s.groupRaftServer
	leaderNodeId, host, port, err := s.GetGroupLeaderHostname()
	if err != nil {
		return err
	}

	// Forward to leader if we're not the leader.
	if leaderNodeId != s.nodeId {
		return rpc(host, port, "POST", fmt.Sprintf("/cluster/groups/%s/commands", s.nodeGroupId), command, nil)
	}

	// Apply to this node if we're leader.
	err = raftServer.Do(command)
	return err
}

//--------------------------------------
// Log Reset
//--------------------------------------

// Resets the server so it is in a mode that it can join a cluster. To join a
// cluster, the server must not have any log entries (e.g. cluster config,
// schema changes, data).
func (s *Server) Reset() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Ensure that the last cluster log entry was an "init" marker command.
	if s.clusterRaftServer.LastCommandName() != "init" {
		return errors.New("skyd: Cannot reset server that has existing log")
	}
	if s.clusterRaftServer.LastCommandName() != "init" {
		return errors.New("skyd: Cannot reset server that has existing log")
	}

	// Maintain the name of the server and close it.
	s.closeClusterRaftServer()

	// Clear the cluster raft directory.
	if err := os.RemoveAll(s.ClusterRaftPath()); err != nil {
		panic("skyd: Unable to clear cluster log before join")
	}

	// Reinitialize the cluster raft server with no log initialization.
	if err := s.initClusterRaftServer(false); err != nil {
		panic("skyd: Unable to reinitialize for join")
	}

	return nil
}

//--------------------------------------
// Raft Transport
//--------------------------------------

// Sends AppendEntries RPCs to a peer when the server is the leader.
func (s *Server) SendAppendEntriesRequest(raftServer *raft.Server, peer *raft.Peer, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	// Find connection info.
	host, port, err := s.cluster.GetNodeHostname(peer.Name())
	if err != nil {
		return nil, fmt.Errorf("skyd: Peer not found: %s", peer.Name())
	}

	// Send the entries over HTTP.
	resp := &raft.AppendEntriesResponse{}
	err = rpc(host, port, "POST", "/cluster/append", req, resp)
	return resp, err
}

// Sends RequestVote RPCs to a peer when the server is the candidate.
func (s *Server) SendVoteRequest(raftServer *raft.Server, peer *raft.Peer, req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	// Find connection info.
	host, port, err := s.cluster.GetNodeHostname(peer.Name())
	if err != nil {
		return nil, fmt.Errorf("skyd: Peer not found: %s", peer.Name())
	}

	// Send the entries over HTTP.
	resp := &raft.RequestVoteResponse{}
	err = rpc(host, port, "POST", "/cluster/vote", req, resp)
	return resp, err
}

// Sends a Snapshot RPC to a peer when the server is the leader.
func (s *Server) SendSnapshotRequest(raftServer *raft.Server, peer *raft.Peer, req *raft.SnapshotRequest) (*raft.SnapshotResponse, error) {
	panic("skyd: snapshot not implemented yet")
	return nil, nil
}

