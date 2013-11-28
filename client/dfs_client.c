#include "client/dfs_client.h"
#include "datanode/ext.h"

int connect_to_nn(char* address, int port)
{
	assert(address != NULL);
	assert(port >= 1 && port <= 65535);
	//TODO: create a socket and connect it to the server (address, port)
	//assign return value to client_socket 
	int client_socket = create_client_tcp_socket(address, port);
    
	return client_socket;
}

int modify_file(char *ip, int port, const char* filename, int file_size, int start_addr, int end_addr)
{
	int namenode_socket = connect_to_nn(ip, port);
	if (namenode_socket == INVALID_SOCKET) return -1;
	FILE* file = fopen(filename, "rb");
	assert(file != NULL);

	//TODO:fill the request and send
	dfs_cm_client_req_t request;
	
	//TODO: receive the response
	dfs_cm_file_res_t response;

	//TODO: send the updated block to the proper datanode

	fclose(file);
	return 0;
}

int push_file(int namenode_socket, const char* local_path)
{
	assert(namenode_socket != INVALID_SOCKET);
	assert(local_path != NULL);
	FILE* file = fopen(local_path, "rb");
	assert(file != NULL);

	// Create the push request
	dfs_cm_client_req_t request;
    
	//TODO:fill the fields in request and
    request.req_type = 1;
    memcpy(request.file_name, local_path, sizeof(request.file_name));
    fseek(file, 0, SEEK_END);
    int size = ftell(file);
    rewind(file);
    request.file_size = size;
    send_data(namenode_socket, &request, sizeof(request));
    printf("SUCCESS: push_file request was sent\n");
	
	//TODO:Receive the response
	dfs_cm_file_res_t response;
    receive_data(namenode_socket, &response, sizeof(response));
    printf("SUCCESS: got push_file response\n");

	//TODO: Send blocks to datanodes one by one
    int i;
    int dataSocket;
    for(i = 0; i < response.query_result.blocknum; i++){
        dfs_cli_dn_req_t dataReq;
        dataReq.op_type = 1;
        memcpy(&dataReq.block, &response.query_result.block_list[i], sizeof(response.query_result.block_list[i]));
//        char *temp = malloc(DFS_BLOCK_SIZE);
        fread(&dataReq.block.content, DFS_BLOCK_SIZE, 1, file);
//        memcpy(&dataReq.block.content, temp, sizeof(temp));
        dataSocket = connect_to_nn(dataReq.block.loc_ip, dataReq.block.loc_port);
        printf("%s %d\n%s\n", dataReq.block.owner_name, dataReq.block.block_id, dataReq.block.content);
        send_data(dataSocket, &dataReq, sizeof(dataReq));
    }
    printf("SUCCESS: push_file data was sent\n");

	fclose(file);
	return 0;
}

int pull_file(int namenode_socket, const char *filename)
{
	assert(namenode_socket != INVALID_SOCKET);
	assert(filename != NULL);

	//TODO: fill the request, and send
	dfs_cm_client_req_t request;
    request.req_type = 0;
    memcpy(request.file_name, local_path, sizeof(request.file_name));
    send_data(namenode_socket, &request, sizeof(request));
    printf("SUCCESS: pull_file request was sent\n");

	//TODO: Get the response
	dfs_cm_file_res_t response;
    receive_data(namenode_socket, &response, sizeof(response));
    printf("SUCCESS: got pull_file response\n");
	
	//TODO: Receive blocks from datanodes one by one
    int i;
    int dataSocket;
    int fileSize = response.query_result.file_size;
    (char *)buf = malloc(fileSize);
    int cursor = 0;
    for(i = 0; i < response.query_result.blocknum; i++){
        dfs_cli_dn_req_t dataReq;
        dataReq.op_type = 0;
        memcpy(&dataReq.block, &response.query_result.block_list[i], sizeof(response.query_result.block_list[i]));
        dataSocket = connect_to_nn(dataReq.block.loc_ip, dataReq.block.loc_port);
        send_data(dataSocket, &dataReq, sizeof(dataReq));
        printf("SUCCESS: pull_file data request sent\n");
        
        dfs_cm_block_t dataRes;
        receive_data(dataSocket, &dataRes, sizeof(dfs_cm_block_t));
        memcpy(buf + cursor, &dataRes.content, DFS_BLOCK_SIZE);
        cursor += DFS_BLOCK_SIZE;
    }
	
	FILE *file = fopen(filename, "wb");
	//TODO: resemble the received blocks into the complete file
    fwrite(buf, 1, fileSize, file);
	fclose(file);
	return 0;
}

dfs_system_status *get_system_info(int namenode_socket)
{
	assert(namenode_socket != INVALID_SOCKET);
	//TODO fill the result and send 
	dfs_cm_client_req_t request;
    request.req_type = 2;
    send_data(namenode_socket, &request, sizeof(request));
    printf("SUCCESS: get_system_info request was sent\n");
    
	//TODO: get the response
	dfs_system_status *response = malloc(sizeof(dfs_system_status));
    receive_data(namenode_socket, response, sizeof(dfs_system_status));
    
    printf("SUCCESS: got get_system_info response\n");
	return response;
}

int send_file_request(char **argv, char *filename, int op_type)
{
	int namenode_socket = connect_to_nn(argv[1], atoi(argv[2]));
	if (namenode_socket < 0)
	{
		return -1;
	}

	int result = 1;
	switch (op_type)
	{
		case 0:
			result = pull_file(namenode_socket, filename);
			break;
		case 1:
			result = push_file(namenode_socket, filename);
			break;
	}
	close(namenode_socket);
	return result;
}

dfs_system_status *send_sysinfo_request(char **argv)
{
	int namenode_socket = connect_to_nn(argv[1], atoi(argv[2]));
	if (namenode_socket < 0)
	{
		return NULL;
	}
	dfs_system_status* ret =  get_system_info(namenode_socket);
	close(namenode_socket);
	return ret;
}
