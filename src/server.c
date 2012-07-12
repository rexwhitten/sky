#include <inttypes.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unistd.h>

#include "bstring.h"
#include "server.h"
#include "message.h"
#include "dbg.h"


//==============================================================================
//
// Functions
//
//==============================================================================

//======================================
// Table management
//======================================

// Opens an table.
//
// server - The server that is opening the table.
// database_name - The name of the database to open.
// table_name - The name of the table to open.
// database - Returns the instance of the database to the caller. 
// table - Returns the instance of the table to the caller. 
//
// Returns 0 if successful, otherwise returns -1.
int open_table(sky_server *server, bstring database_name,
                     bstring table_name, sky_database **database,
                     sky_table **table)
{
    int rc;
    
    // Determine the path to the database.
    bstring path = bformat("%s/%s", bdata(server->path), bdata(database_name)); 
    
    // Create the database.
    *database = sky_database_create(path);
    check_mem(*database);
    
    // Create the table.
    *table = sky_table_create(*database, table_name);
    check_mem(*table);
    
    // Open the table.
    rc = sky_table_open(*table);
    check(rc == 0, "Unable to open table");
    
    return 0;

error:
    sky_database_free(*database);
    sky_table_free(*table);
    *database = NULL;
    *table = NULL;
    return -1;
}

// Closes an table.
//
// server - The server that is opening the table.
// database - The database that the table belongs to.
// table - The table to close.
//
// Returns 0 if successful, otherwise returns -1.
int close_table(sky_server *server, sky_database *database,
                      sky_table *table)
{
    int rc;
    
    // HACK: Suppress unused "server" variable warning for now.
    server = server;
    
    // Close the table.
    rc = sky_table_close(table);
    check(rc == 0, "Unable to close table");

    // Free the table.
    sky_table_free(table);
    
    // Free the database file.
    sky_database_free(database);
    
    return 0;

error:
    sky_table_free(table);
    sky_database_free(database);
    return -1;
}


//======================================
// Lifecycle
//======================================

// Creates a reference to a server instance.
//
// path - The directory path where the databases reside.
//
// Returns a reference to the server.
sky_server *sky_server_create(bstring path)
{
    sky_server *server = NULL;
    server = calloc(1, sizeof(sky_server)); check_mem(server);
    
    server->path = path;
    server->port = SKY_DEFAULT_PORT;
    
    return server;

error:
    sky_server_free(server);
    return NULL;
}

// Frees a server instance from memory.
//
// server - The server object to free.
void sky_server_free(sky_server *server)
{
    if(server) {
        if(server->path) bdestroy(server->path);
        free(server);
    }
}


//======================================
// State
//======================================

// Starts a server. Once a server is started, it can accept messages over TCP
// on the bind address and port number specified by the server object.
//
// server - The server to start.
//
// Returns 0 if successful, otherwise returns -1.
int sky_server_start(sky_server *server)
{
    int rc;

    check(server != NULL, "Server required");
    check(server->state == SKY_SERVER_STATE_STOPPED, "Server already running");
    check(server->port > 0, "Port required");

    // Initialize socket info.
    server->sockaddr = calloc(1, sizeof(struct sockaddr_in));
    check_mem(server->sockaddr);
    server->sockaddr->sin_addr.s_addr = INADDR_ANY;
    server->sockaddr->sin_port = htons(server->port);
    server->sockaddr->sin_family = AF_INET;

    // Create socket.
    server->socket = socket(AF_INET, SOCK_STREAM, 0);
    check(server->socket != -1, "Unable to create a socket");
    
    // Bind socket.
    rc = bind(server->socket, (struct sockaddr*)server->sockaddr, sizeof(struct sockaddr_in));
    check(rc == 0, "Unable to bind socket");
    
    // Listen on socket.
    rc = listen(server->socket, SKY_LISTEN_BACKLOG);
    check(rc != -1, "Unable to listen on socket");
    
    // Update server state.
    server->state = SKY_SERVER_STATE_RUNNING;
    
    return 0;

error:
    sky_server_stop(server);
    return -1;
}

// Stops a server. This actions closes the TCP socket and in-process messages
// will be aborted.
//
// server - The server to stop.
//
// Returns 0 if successful, otherwise returns -1.
int sky_server_stop(sky_server *server)
{
    // Close socket if open.
    if(server->socket > 0) {
        close(server->socket);
    }
    server->socket = 0;

    // Clear socket info.
    if(server->sockaddr) {
        free(server->sockaddr);
    }
    server->sockaddr = NULL;
    
    // Update server state.
    server->state = SKY_SERVER_STATE_STOPPED;
    
    return 0;
}


//======================================
// Connection Management
//======================================

// Accepts a connection on a running server. Once a connection is accepted then
// the message is parsed and processed.
//
// server - The server to start.
//
// Returns 0 if successful, otherwise returns -1.
int sky_server_accept(sky_server *server)
{
    int rc;
    void *buffer;
    
    // Accept the next connection.
    int sockaddr_size = sizeof(struct sockaddr_in);
    int socket = accept(server->socket, (struct sockaddr*)server->sockaddr, (socklen_t *)&sockaddr_size);
    check(socket != -1, "Unable to accept connection");
    
    // Read message header.
    buffer = calloc(1, SKY_MESSAGE_HEADER_LENGTH);
    rc = read(socket, buffer, SKY_MESSAGE_HEADER_LENGTH);
    check(rc == SKY_MESSAGE_HEADER_LENGTH, "Unable to read message header");
    
    // Parse message header.
    sky_message_header *header = sky_message_header_create();
    check_mem(header);
    rc = sky_message_header_parse(buffer, header);
    check(rc == 0, "Unable to parse message header");
    
    // Extend buffer for length of message.
    uint32_t buffer_length = SKY_MESSAGE_HEADER_LENGTH + header->length;
    buffer = realloc(buffer, buffer_length);
    check_mem(buffer);
    
    // Parse appropriate message type.
    switch(header->type) {
        case SKY_MESSAGE_EADD: {
            rc = sky_server_process_eadd_message(server, socket, buffer);
            check(rc == 0, "Unable to process EADD message");
            break;
        }
        
        default: {
            sentinel("Invalid message type");
            break;
        }
    }
    
    // Clean up.
    if(header) sky_message_header_free(header);
    close(socket);

    return 0;

error:
    if(header) sky_message_header_free(header);
    if(socket > 0) close(socket);
    return -1;
}


//======================================
// Message Processing
//======================================

// Processes an "Event Add" message.
//
// server - The server that received the message.
// socket - The socket that sent the message.
// buffer - The buffer that contains the full message.
//
// Returns 0 if successful, otherwise returns -1.
int sky_server_process_eadd_message(sky_server *server, int socket,
                                    void *buffer)
{
    int rc;
    
    // Parse message from buffer.
    sky_eadd_message *message = sky_eadd_message_create();
    rc = sky_eadd_message_parse(buffer, message);
    
    // Validate message.
    check(server->path != NULL, "Server path is required");
    check(message->database_name != NULL, "Database name is required");
    check(message->table_name != NULL, "Table name is required");
    check(message->object_id != 0, "Object ID is required");
    
    // Open table.
    sky_database *database;
    sky_table *table;
    open_table(server, message->database_name, message->table_name,
                     &database, &table);

    // Look up action.
    sky_action_id_t action_id = 0;
    if(message->action_name != NULL) {
        sky_action *action = NULL;
        rc = sky_action_file_find_action_by_name(table->action_file, message->action_name, &action);
        check(rc == 0 && action != NULL, "Action does not exist: '%s'", bdata(message->action_name));
        action_id = action->id;
    }

    // Create event.
    sky_event *event = sky_event_create(message->timestamp, message->object_id, action_id);
    
    // Add data to event.
    int i;
    for(i=0; i<message->data_count; i++) {
        // Look up key.
        //sky_property_id_t property_id;
        //rc = sky_table_find_or_create_property_id_by_name(table, message->data_keys[i], &property_id);
        //check(rc == 0, "Unable to find or create property id: %s", bdata(message->data_keys[i]));
        
        // Add event data.
        //sky_event_set_data(event, property_id, message->data_values[i]);
    }
    
    // Add event to table.
    //rc = sky_table_add_event(table, event);
    //check(rc == 0, "Unable to add event to table");
    
    // TODO: Send respond to socket.
    
    // Close table.
    close_table(server, database, table);

    // Clean up.
    sky_event_free(event);
    sky_eadd_message_free(message);

    return 0;

error:
    //sky_event_free(event);
    sky_eadd_message_free(message);
    return -1;
}

