#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <netdb.h>
#include <time.h>
#include <sys/select.h> 

#define MAX_CONN 30
#define BUF_SIZE 512
#define MAX_NAME 32
#define MAX_DATA 107

#define OP_ADD 43
#define OP_MUL 42
#define OP_MOD 37

// Typy połączenia
#define TYPE_C 'C'
#define TYPE_T 'T'
#define TYPE_U 'U'


struct connection {
    int fd;
    struct sockaddr_in addr;
    socklen_t addrlen;
    int is_used;
    int type;
    int client_id;
    int conn_id;
    int is_command;
    int is_listener;
    char name[64];
};

fd_set master_set;
int fdmax = 0;
int listener_fd;
struct connection connections[MAX_CONN];
int next_client_id = 0;
int next_conn_id = 1;

void handle_new_connection(int index, char type, int base_port, fd_set *master, int *maxfd);
void process_binary_data(int index);
void process_udp_data(int index);

void init_connections() {
    for (int i = 0; i < MAX_CONN; i++) {
        connections[i].is_used = 0;
        connections[i].fd = -1;
    }
}

int find_free_slot() {
    for (int i = 0; i < MAX_CONN; i++) {
        if (!connections[i].is_used)
            return i;
    }
    return -1;
}

void send_msg(int fd, const char* msg) {
    send(fd, msg, strlen(msg), 0);

}

int count_active_connections() {
    int count = 0;
    for (int i = 0; i < MAX_CONN; i++) {
        if (connections[i].is_used)
            count++;
    }
    return count;
}

int count_free_connections() {
    return MAX_CONN - count_active_connections();
}

void broadcast_info(unsigned short client_id, const char* info) {
    char msg[256];
    snprintf(msg, sizeof(msg), "#INF@INFO:%s;!", info);

    for (int i = 0; i < MAX_CONN; i++) {
        if (connections[i].is_used &&
            connections[i].type == TYPE_C &&
            connections[i].client_id == client_id) {
            printf("[INFO] to fd=%d (client_id=%d, name=%s): %s\n",
                   connections[i].fd, client_id, connections[i].name, msg);
            send_msg(connections[i].fd, msg);
        }
    }
}


void handle_name_command(int index, const char* name_value) {
    strncpy(connections[index].name, name_value, MAX_NAME - 1);
    connections[index].client_id = next_client_id++;
    connections[index].conn_id = next_conn_id++;
    connections[index].is_command = 1;
    connections[index].type = TYPE_C;
    connections[index].is_used = 1;

    char msg[256];
    snprintf(msg, sizeof(msg), "#NOK@CONNECTION:%d;!", index);
    send_msg(connections[index].fd, msg);
    printf("Client %s connected.\n", name_value);
}

void handle_active_query(int index) {
    char msg[128];
    snprintf(msg, sizeof(msg), "#AOK@ALL:%d;!", count_active_connections());
    send_msg(connections[index].fd, msg);
}

void handle_free_query(int index) {
    char msg[128];
    snprintf(msg, sizeof(msg), "#DOK@FREE:%d;!", count_free_connections());
    send_msg(connections[index].fd, msg);
}

void handle_disconnect(int index, int conn_id) {
    for (int i = 0; i < MAX_CONN; i++) {
        if (connections[i].is_used && connections[i].conn_id == conn_id && connections[i].client_id == connections[index].client_id) {
            close(connections[i].fd);
            connections[i].is_used = 0;
            char msg[128];
            snprintf(msg, sizeof(msg), "#KOK@INFO:%d closed;!", conn_id);
            send_msg(connections[index].fd, msg);
            return;
        }
    }
    send_msg(connections[index].fd, "#KER@ERROR:connection not found;!");
}

void handle_command(int index, const char* buf) {
    printf("[%s] Received command: %s\n", connections[index].name, buf);

    if (strncmp(buf, "#N@NAME:", 8) == 0) {
        char name[MAX_NAME];
        sscanf(buf + 8, "%[^;];!", name);
        handle_name_command(index, name);
    } else if (strncmp(buf, "#K@CONNECTION:", 15) == 0) {
        int conn_id = atoi(buf + 15);
        handle_disconnect(index, conn_id);

    } else if (strncmp(buf, "#T@!", 4) == 0) {
        handle_new_connection(index, TYPE_T, 6000, &master_set, &fdmax);
        printf("[%s] handle_command: request to open T\n", connections[index].name);

    } else if (strncmp(buf, "#U@!", 4) == 0) {
        handle_new_connection(index, TYPE_U, 7000, &master_set, &fdmax);
        printf("[%s] handle_command: request to open U\n", connections[index].name);

    }
}

void handle_new_connection(int index, char type, int base_port, fd_set *master, int *maxfd) {
    int new_fd, port;
    struct sockaddr_in new_addr;
    socklen_t addrlen = sizeof(new_addr);

    for (port = base_port + 1; port < base_port + 1000; port++) {
        new_fd = socket(AF_INET, (type == TYPE_T ? SOCK_STREAM : SOCK_DGRAM), 0);
        if (new_fd < 0) continue;

        int opt = 1;
        setsockopt(new_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        memset(&new_addr, 0, sizeof(new_addr));
        new_addr.sin_family = AF_INET;
        new_addr.sin_addr.s_addr = INADDR_ANY;
        new_addr.sin_port = htons(port);

        if (bind(new_fd, (struct sockaddr*)&new_addr, addrlen) == 0) break;
        close(new_fd);
    }

    if (port >= base_port + 1000) {
        send_msg(connections[index].fd, "#INF@INFO:No free port;!");
        return;
    }

    if (type == TYPE_T && listen(new_fd, 10) < 0) {
        perror("listen failed");
        close(new_fd);
        return;
    }

    int slot = find_free_slot();
    if (slot == -1) {
        close(new_fd);
        const char *prefix = (type == TYPE_T) ? "#TER@" : "#UER@";
        char msg[128];
        snprintf(msg, sizeof(msg), "%sERROR:No free connection slot;!", prefix);
        send_msg(connections[index].fd, msg);
        return;
    }

    connections[slot].fd = new_fd;
    connections[slot].addr = new_addr;
    connections[slot].addrlen = addrlen;
    connections[slot].is_used = 1;
    connections[slot].type = type;
    connections[slot].is_command = 0;
    connections[slot].client_id = connections[index].client_id;
    connections[slot].conn_id = next_conn_id++;
    connections[slot].is_listener = 1;


    FD_SET(new_fd, master);
    if (new_fd > *maxfd) *maxfd = new_fd;
    printf("Added socket %d to master set for %c\n", new_fd, type);

    const char *prefix = (type == TYPE_T) ? "#TOK@" : "#UOK@";
    char response[128];
    snprintf(response, sizeof(response), "%sPORT:%d;CONNECTION:%d;!", prefix, port, connections[slot].conn_id);
    send_msg(connections[index].fd, response);
    printf("[%s] new connection %c is waiting on port %d (conn_id=%d)\n", connections[index].name, type, port, connections[slot].conn_id);
}

void process_binary_data(int index) {
    uint32_t header[7];
    int fd = connections[index].fd;

    int n = recv(fd, header, sizeof(header), MSG_WAITALL);
    if (n != sizeof(header)) {
        broadcast_info(connections[index].client_id, "Header recv failed");
        return;
    }

    for (int i = 0; i < 7; i++) header[i] = ntohl(header[i]);

    if (header[0] != 0) return;

    uint32_t count = header[2];
    uint32_t idx1 = header[3];
    uint32_t idx2 = header[4];
    uint32_t idx_res = header[5];
    uint32_t op = header[6];

    if (count == 0 || count > 1024 || idx1 >= count || idx2 >= count || idx_res >= count) {
        broadcast_info(connections[index].client_id, "Invalid header");
        return;
    }

    uint32_t *data = malloc(count * sizeof(uint32_t));
    if (!data) {
        broadcast_info(connections[index].client_id, "Memory error");
        return;
    }

    n = recv(fd, data, count * sizeof(uint32_t), MSG_WAITALL);
    if (n != (int)(count * sizeof(uint32_t))) {
        free(data);
        broadcast_info(connections[index].client_id, "Data recv failed");
        return;
    }

    for (uint32_t i = 0; i < count; i++) data[i] = ntohl(data[i]);

    uint32_t a = data[idx1];
    uint32_t b = data[idx2];
    uint32_t res = 0;

    if (op == 43) res = a + b;
    else if (op == 42) res = a * b;
    else if (op == 37 && b != 0) res = a % b;
    else res = 0xFFFFFFFF;

    data[idx_res] = res;

    for (uint32_t i = 0; i < count; i++) data[i] = htonl(data[i]);
    for (int i = 0; i < 7; i++) header[i] = htonl(header[i]);

    send(fd, header, sizeof(header), 0);
    send(fd, data, count * sizeof(uint32_t), 0);

    broadcast_info(connections[index].client_id, "Computation OK");

    free(data);
}




void process_udp_data(int index) {
    struct sockaddr_in client;
    socklen_t len = sizeof(client);
    char buf[BUF_SIZE];

    int n = recvfrom(connections[index].fd, buf, BUF_SIZE, 0, (struct sockaddr*)&client, &len);
    if (n < (int)(sizeof(uint32_t) * 7)) return;

    uint32_t *pkt = (uint32_t*)buf;
    for (int i = 0; i < 7; i++) pkt[i] = ntohl(pkt[i]);

    if (pkt[0] != 0) return;

    int count = pkt[2];
    int idx1 = pkt[3];
    int idx2 = pkt[4];
    int idx_res = pkt[5];
    int op = pkt[6];

    if (count > MAX_DATA || n < (int)((7 + count) * sizeof(uint32_t))) return;

    uint32_t *data = pkt + 7;
    for (int i = 0; i < count; i++) data[i] = ntohl(data[i]);

    uint32_t a = data[idx1];
    uint32_t b = data[idx2];
    uint32_t res = 0;

    if (op == 43) res = a + b;
    else if (op == 42) res = a * b;
    else if (op == 37 && b != 0) res = a % b;
    else return;

    data[idx_res] = res;

    for (int i = 0; i < count; i++) data[i] = htonl(data[i]);
    for (int i = 0; i < 7; i++) pkt[i] = htonl(pkt[i]);

    sendto(connections[index].fd, pkt, (7 + count) * sizeof(uint32_t), 0, (struct sockaddr*)&client, len);
}

int main() {
    listener_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listener_fd < 0) {
        perror("socket");
        exit(1);
    }

    int opt = 1;
    setsockopt(listener_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(3000);

    if (bind(listener_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind");
        exit(1);
    }

    if (listen(listener_fd, 10) < 0) {
        perror("listen");
        exit(1);
    }

    FD_ZERO(&master_set);
    fdmax = listener_fd;
    FD_SET(listener_fd, &master_set);

    while (1) {
        fd_set read_fds = master_set;
        if (select(fdmax + 1, &read_fds, NULL, NULL, NULL) < 0) {
            perror("select");
            continue;
        }

        for (int i = 0; i <= fdmax; i++) {
            if (!FD_ISSET(i, &read_fds)) continue;

            if (i == listener_fd) {
                struct sockaddr_in client_addr;
                socklen_t addrlen = sizeof(client_addr);
                int newfd = accept(listener_fd, (struct sockaddr*)&client_addr, &addrlen);
                if (newfd >= 0) {
                    int slot = find_free_slot();
                    if (slot != -1) {
                        connections[slot].fd = newfd;
                        connections[slot].addr = client_addr;
                        connections[slot].addrlen = addrlen;
                        connections[slot].is_used = 1;
                        connections[slot].type = TYPE_C;
                        connections[slot].is_command = 1;
                        connections[slot].is_listener = 0;
                        connections[slot].client_id = -1;
                        connections[slot].conn_id = -1;
                        FD_SET(newfd, &master_set);
                        if (newfd > fdmax) fdmax = newfd;
                        printf("New C connection accepted on fd=%d\n", newfd);
                    } else close(newfd);
                }
            } else {
                for (int j = 0; j < MAX_CONN; j++) {
                    if (connections[j].is_used && connections[j].fd == i) {
                        if (connections[j].type == TYPE_C) {
                            char buf[BUF_SIZE];
                            int nbytes = recv(i, buf, sizeof(buf) - 1, 0);
                            if (nbytes <= 0) {
                                close(i);
                                FD_CLR(i, &master_set);
                                connections[j].is_used = 0;
                                printf("[C] Closed fd=%d\n", i);
                            } else {
                                buf[nbytes] = '\0';
                                printf("[%s] handle_command received: %s\n", connections[j].name, buf);
                                handle_command(j, buf);
                            }
                        } else if (connections[j].type == TYPE_T && connections[j].is_listener) {
                            // nasłuchujące T
                            struct sockaddr_in client_addr;
                            socklen_t addrlen = sizeof(client_addr);
                            int newfd = accept(i, (struct sockaddr*)&client_addr, &addrlen);
                            if (newfd >= 0) {
                                int slot = find_free_slot();
                                if (slot != -1) {
                                    connections[slot].fd = newfd;
                                    connections[slot].addr = client_addr;
                                    connections[slot].addrlen = addrlen;
                                    connections[slot].is_used = 1;
                                    connections[slot].type = TYPE_T;
                                    connections[slot].is_command = 0;
                                    connections[slot].is_listener = 0;
                                    connections[slot].client_id = connections[j].client_id;
                                    connections[slot].conn_id = connections[j].conn_id;
                                    FD_SET(newfd, &master_set);
                                    if (newfd > fdmax) fdmax = newfd;
                                    printf("[T] accepted data connection fd=%d (client_id=%d conn_id=%d)\n",
                                           newfd, connections[slot].client_id, connections[slot].conn_id);
                                } else close(newfd);
                            }
                        } else if (connections[j].type == TYPE_T && !connections[j].is_listener) {
                            process_binary_data(j);
                        } else if (connections[j].type == TYPE_U) {
                            process_udp_data(j);
                        }
                        break;
                    }
                }
            }
        }
    }
    return 0;
}